-module(ar_packing_server).

-behaviour(gen_server).

-export([start_link/0, packing_atom/1,
		 request_unpack/2, request_repack/2, pack/4, unpack/5, repack/6, 
		 is_buffer_full/0, record_buffer_size_metric/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

%% Only used by ar_bench_packing.erl
-export([chunk_key/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%% The packing latency as it is chosen for the protocol.
-define(PACKING_LATENCY_MS, 60).

-record(state, {
	workers,
	num_workers
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================
packing_atom(Packing) when is_atom(Packing) ->
	Packing;
packing_atom({spora_2_6, _}) ->
	spora_2_6.

request_unpack(Ref, Args) ->
	gen_server:cast(?MODULE, {unpack_request, self(), Ref, Args}).

request_repack(Ref, Args) ->
	gen_server:cast(?MODULE, {repack_request, self(), Ref, Args}).

%% @doc Pack the chunk for mining. Packing ensures every mined chunk of data is globally
%% unique and cannot be easily inferred during mining from any metadata stored in RAM.
pack(Packing, ChunkOffset, TXRoot, Chunk) ->
	[{_, RandomXStateRef}] = ets:lookup(?MODULE, randomx_packing_state),
	record_packing_request(pack, Packing, get_caller()),
	case pack(Packing, ChunkOffset, TXRoot, Chunk, RandomXStateRef, external) of
		{ok, Packed, _} ->
			{ok, Packed};
		Reply ->
			Reply
	end.

%% @doc Unpack the chunk packed for mining. Return {ok, UnpackedChunk},
%% {error, invalid_packed_size}, {error, invalid_chunk_size}, or {error, invalid_padding}.
unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
	[{_, RandomXStateRef}] = ets:lookup(?MODULE, randomx_packing_state),
	record_packing_request(unpack, Packing, get_caller()),
	case unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, external) of
		{ok, Unpacked, _} ->
			{ok, Unpacked};
		Reply ->
			Reply
	end.

repack(RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
	[{_, RandomXStateRef}] = ets:lookup(?MODULE, randomx_packing_state),
	record_packing_request(repack, RequestedPacking, get_caller()),
	repack(
		RequestedPacking, StoredPacking, ChunkOffset, TXRoot,
		Chunk, ChunkSize, RandomXStateRef, external).

%% @doc Return true if the packing server buffer is considered full, to apply
%% some back-pressure on the pack/4 and unpack/5 callers.
is_buffer_full() ->
	[{_, Limit}] = ets:lookup(?MODULE, buffer_size_limit),
	case ets:lookup(?MODULE, buffer_size) of
		[{_, Size}] when Size > Limit ->
			true;
		_ ->
			false
	end.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	ar:console("~nInitialising RandomX dataset for fast packing. Key: ~p. "
			"The process may take several minutes.~n", [ar_util:encode(?RANDOMX_PACKING_KEY)]),
	PackingStateRef = ar_mine_randomx:init_fast(?RANDOMX_PACKING_KEY, Schedulers),
	ar:console("RandomX dataset initialisation complete.~n", []),
	ets:insert(?MODULE, {randomx_packing_state, PackingStateRef}),
	{ActualRatePack_2_5, ActualRatePack_2_6,
			ActualRateUnpack_2_5, ActualRateUnpack_2_6} = get_packing_latency(PackingStateRef),
	PackingLatency = ActualRatePack_2_6,
	MaxRate = Schedulers * 1000 / PackingLatency,
	TheoreticalMaxRate = Schedulers * 1000 / (?PACKING_LATENCY_MS),
	{PackingRate, SchedulersRequired} =
		case Config#config.packing_rate of
			undefined ->
				ChosenRate = max(1, ceil(2 * MaxRate / 3)),
				ChosenRate2 = ar_util:ceil_int(ChosenRate, 10),
				log_packing_rate(ChosenRate2, MaxRate),
				SchedulersRequired2 = ceil(ChosenRate2 / (1000 / (?PACKING_LATENCY_MS))),
				{ChosenRate2, SchedulersRequired2};
			ConfiguredRate ->
				SchedulersRequired2 = ceil(ConfiguredRate / (1000 / PackingLatency)),
				case SchedulersRequired2 > Schedulers of
					true ->
						log_insufficient_core_count(Schedulers, ConfiguredRate, MaxRate);
					false ->
						log_packing_rate(ConfiguredRate, MaxRate)
				end,
				{ConfiguredRate, SchedulersRequired2}
		end,
	record_packing_benchmarks({TheoreticalMaxRate, PackingRate, Schedulers,
			ActualRatePack_2_5, ActualRatePack_2_6, ActualRateUnpack_2_5,
			ActualRateUnpack_2_6}),
	SpawnSchedulers = min(SchedulersRequired, Schedulers),
	%% Since the total rate of spawned processes might exceed the desired rate,
	%% artificially throttle processes uniformly.
	ThrottleDelay = calculate_throttle_delay(SpawnSchedulers, PackingRate),
	Workers = queue:from_list(
		[spawn_link(fun() -> worker(ThrottleDelay, PackingStateRef) end)
			|| _ <- lists:seq(1, SpawnSchedulers)]),
	ets:insert(?MODULE, {buffer_size, 0}),
	{ok, Config} = application:get_env(arweave, config),
	MaxSize =
		case Config#config.packing_cache_size_limit of
			undefined ->
				Free = proplists:get_value(free_memory, memsup:get_system_memory_data(),
						2000000000),
				Limit2 = min(1200, erlang:ceil(Free * 0.9 / 3 / 262144)),
				Limit3 = ar_util:ceil_int(Limit2, 100),
				Limit3;
			Limit ->
				Limit
		end,
	ar:console("~nSetting the packing chunk cache size limit to ~B chunks.~n", [MaxSize]),
	ets:insert(?MODULE, {buffer_size_limit, MaxSize}),
	timer:apply_interval(200, ?MODULE, record_buffer_size_metric, []),
	{ok, #state{ 
		workers = Workers, num_workers = SpawnSchedulers }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({unpack_request, _, _, _}, #state{ num_workers = 0 } = State) ->
	?LOG_WARNING([{event, got_unpack_request_while_packing_is_disabled}]),
	{noreply, State};
handle_cast({unpack_request, From, Ref, Args}, State) ->
	#state{ workers = Workers } = State,
	{Packing, _Chunk, _AbsoluteOffset, _TXRoot, _ChunkSize} = Args,
	{{value, Worker}, Workers2} = queue:out(Workers),
	increment_buffer_size(),
	record_packing_request(unpack, Packing, unpack_request),
	Worker ! {unpack, Ref, From, Args},
	{noreply, State#state{ workers = queue:in(Worker, Workers2) }};
handle_cast({repack_request, _, _, _}, #state{ num_workers = 0 } = State) ->
	?LOG_WARNING([{event, got_repack_request_while_packing_is_disabled}]),
	{noreply, State};
handle_cast({repack_request, From, Ref, Args}, State) ->
	#state{ workers = Workers } = State,
	{RequestedPacking, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
	{{value, Worker}, Workers2} = queue:out(Workers),
	case {RequestedPacking, Packing} of
		{unpacked, unpacked} ->
			From ! {chunk, {packed, Ref, {unpacked, Chunk, AbsoluteOffset, TXRoot, ChunkSize}}},
			{noreply, State};
		{_, unpacked} ->
			increment_buffer_size(),
			record_packing_request(pack, RequestedPacking, repack_request),
			Worker ! {pack, Ref, From, {RequestedPacking, Chunk, AbsoluteOffset, TXRoot,
					ChunkSize}},
			{noreply, State#state{ workers = queue:in(Worker, Workers2) }};
		_ ->
			increment_buffer_size(),
			record_packing_request(repack, RequestedPacking, repack_request),
			Worker ! {
				repack, Ref, From,
				{RequestedPacking, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}
			},
			{noreply, State#state{ workers = queue:in(Worker, Workers2) }}
	end;
handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

log_insufficient_core_count(Schedulers, PackingRate, Max) ->
	ar:console("~nThe number of cores on your machine (~B) is not sufficient for "
		"packing ~B chunks per second. Estimated maximum rate: ~.2f chunks/s.~n",
		[Schedulers, PackingRate, Max]),
	?LOG_WARNING([{event, insufficient_core_count_to_sustain_desired_packing_rate},
			{cores, Schedulers}, {packing_rate, PackingRate}]).

log_packing_rate(PackingRate, Max) ->
	ar:console("~nThe node is configured to pack around ~B chunks per second. "
			"To increase the packing rate, start with `packing_rate [number]`. "
			"Estimated maximum rate: ~.2f chunks/s.~n",
			[PackingRate, Max]).

calculate_throttle_delay(0, _PackingRate) ->
	0;
calculate_throttle_delay(_SpawnSchedulers, 0) ->
	0;
calculate_throttle_delay(SpawnSchedulers, PackingRate) ->
	Load = PackingRate / (SpawnSchedulers * (1000 / (?PACKING_LATENCY_MS))),
	case Load >= 1 of
		true ->
			0;
		false ->
			trunc((1 - Load) * (?PACKING_LATENCY_MS))
	end.

worker(ThrottleDelay, RandomXStateRef) ->
	receive
		{unpack, Ref, From, Args} ->
			{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case unpack(Packing, AbsoluteOffset, TXRoot, Chunk, ChunkSize,
					RandomXStateRef, internal) of
				{ok, U, AlreadyUnpacked} ->
					From ! {chunk, {unpacked, Ref, {Packing, U, AbsoluteOffset, TXRoot,
							ChunkSize}}},
					case AlreadyUnpacked of
						already_unpacked ->
							ok;
						_ ->
							timer:sleep(ThrottleDelay)
					end;
				{error, invalid_packed_size} ->
					?LOG_WARNING([{event, got_packed_chunk_of_invalid_size}]);
				{error, invalid_chunk_size} ->
					?LOG_WARNING([{event, got_packed_chunk_with_invalid_chunk_size}]);
				{error, invalid_padding} ->
					?LOG_WARNING([{event, got_packed_chunk_with_invalid_padding},
						{absolute_end_offset, AbsoluteOffset}]);
				{exception, Error} ->
					?LOG_ERROR([{event, failed_to_unpack_chunk},
							{absolute_end_offset, AbsoluteOffset},
							{error, io_lib:format("~p", [Error])}])
			end,
			decrement_buffer_size(),
			worker(ThrottleDelay, RandomXStateRef);
		{pack, Ref, From, Args} ->
			{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case pack(Packing, AbsoluteOffset, TXRoot, Chunk, RandomXStateRef, internal) of
				{ok, Packed, AlreadyPacked} ->
					From ! {chunk, {packed, Ref, {Packing, Packed, AbsoluteOffset, TXRoot,
							ChunkSize}}},
					case AlreadyPacked of
						already_packed ->
							ok;
						_ ->
							timer:sleep(ThrottleDelay)
					end;
				{error, invalid_unpacked_size} ->
					?LOG_WARNING([{event, got_unpacked_chunk_of_invalid_size}]);
				{exception, Error} ->
					?LOG_ERROR([{event, failed_to_pack_chunk},
							{absolute_end_offset, AbsoluteOffset},
							{error, io_lib:format("~p", [Error])}])
			end,
			decrement_buffer_size(),
			worker(ThrottleDelay, RandomXStateRef);
		{repack, Ref, From, Args} ->
			{RequestedPacking, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case repack(RequestedPacking, Packing, 
					AbsoluteOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, internal) of
				{ok, Packed, Unpacked} ->
					From ! {chunk, {packed, Ref, {RequestedPacking, Packed, AbsoluteOffset, TXRoot,
							ChunkSize}}},
					case Unpacked of
						none ->
							%% When RequestdPacking and Packing are the same and neither is
							%% unpacked, then the repack does no work and just returns
							%% the original chunk. In this case we don't need a throttle.
							ok;
						_ ->
							timer:sleep(ThrottleDelay)
					end;
				{error, invalid_packed_size} ->
					?LOG_WARNING([{event, got_packed_chunk_of_invalid_size}]);
				{error, invalid_chunk_size} ->
					?LOG_WARNING([{event, got_packed_chunk_with_invalid_chunk_size}]);
				{error, invalid_padding} ->
					?LOG_WARNING([{event, got_packed_chunk_with_invalid_padding},
						{absolute_end_offset, AbsoluteOffset}]);
				{error, invalid_unpacked_size} ->
					?LOG_WARNING([{event, got_unpacked_chunk_of_invalid_size}]);
				{exception, Error} ->
					?LOG_ERROR([{event, failed_to_repack_chunk},
							{absolute_end_offset, AbsoluteOffset},
							{error, io_lib:format("~p", [Error])}])
			end,
			decrement_buffer_size(),
			worker(ThrottleDelay, RandomXStateRef)
	end.

chunk_key(spora_2_5, ChunkOffset, TXRoot) -> 
	%% The presence of the absolute end offset in the key makes sure
	%% packing of every chunk is unique, even when the same chunk is
	%% present in the same transaction or across multiple transactions
	%% or blocks. The presence of the transaction root in the key
	%% ensures one cannot find data that has certain patterns after
	%% packing.
	{spora_2_5, crypto:hash(sha256, << ChunkOffset:256, TXRoot/binary >>)};
chunk_key({spora_2_6, RewardAddr}, ChunkOffset, TXRoot) -> 
	%% The presence of the absolute end offset in the key makes sure
	%% packing of every chunk is unique, even when the same chunk is
	%% present in the same transaction or across multiple transactions
	%% or blocks. The presence of the transaction root in the key
	%% ensures one cannot find data that has certain patterns after
	%% packing. The presence of the reward address, combined with
	%% the 2.6 mining mechanics, puts a relatively low cap on the performance
	%% of a single dataset replica, essentially incentivizing miners to create
	%% more weave replicas per invested dollar.
	{
		spora_2_6,
		crypto:hash(sha256, << ChunkOffset:256, TXRoot:32/binary, RewardAddr/binary >>)
	}.

pack(unpacked, _ChunkOffset, _TXRoot, Chunk, _RandomXStateRef, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_packed};
pack(PackingArgs, ChunkOffset, TXRoot, Chunk, RandomXStateRef, External) ->
	Packing = packing_atom(PackingArgs),
	case byte_size(Chunk) > ?DATA_CHUNK_SIZE of
		true ->
			{error, invalid_unpacked_size};
		false ->
			{Packing, Key} = chunk_key(PackingArgs, ChunkOffset, TXRoot),
			case prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[pack, Packing, External], fun() ->
							ar_mine_randomx:randomx_encrypt_chunk(Packing, RandomXStateRef, Key,
									Chunk) end) of
				{ok, Packed} ->
					{ok, Packed, was_not_already_packed};
				Error ->
					Error
			end
	end.

unpack(unpacked, _ChunkOffset, _TXRoot, Chunk, _ChunkSize, _RandomXStateRef, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_unpacked};
unpack(PackingArgs, ChunkOffset, TXRoot, Chunk, ChunkSize,
		RandomXStateRef, External) ->
	Packing = packing_atom(PackingArgs),
	case validate_chunk_size(Packing, Chunk, ChunkSize) of
		{error, Reason} ->
			{error, Reason};
		{ok, _PackedSize} ->
			{Packing, Key} = chunk_key(PackingArgs, ChunkOffset, TXRoot),
			case prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[unpack, Packing, External], fun() ->
							ar_mine_randomx:randomx_decrypt_chunk(Packing, RandomXStateRef, Key,
									Chunk, ChunkSize) end) of
				{ok, Unpacked} ->
					{ok, Unpacked, was_not_already_unpacked};
				Error ->
					Error
			end
	end.

repack(unpacked, unpacked,
		_ChunkOffset, _TXRoot, Chunk, _ChunkSize, _RandomXStateRef, _External) ->
	{ok, Chunk, Chunk};
repack(RequestedPacking, unpacked, 
		ChunkOffset, TXRoot, Chunk, _ChunkSize, RandomXStateRef, External) ->
	case pack(RequestedPacking, ChunkOffset, TXRoot, Chunk, RandomXStateRef, External) of
		{ok, Packed, _} ->
			{ok, Packed, Chunk};
		Error ->
			Error
	end;
repack(unpacked, StoredPacking, 
		ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, External) ->
	case unpack(StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, External) of
		{ok, Unpacked, _} ->
			{ok, Unpacked, Unpacked};
		Error ->
			Error
	end;
repack(RequestedPacking, StoredPacking, 
		_ChunkOffset, _TXRoot, Chunk, _ChunkSize, _RandomXStateRef, _External)
		when StoredPacking == RequestedPacking ->
	%% StoredPacking and Packing are in the same format and neither is unpacked. To 
	%% avoid uneccessary unpacking we'll return none for the UnpackedChunk. If a caller
	%% needs the UnpackedChunk they should call unpack explicity.
	{ok, Chunk, none};
repack(RequestedPacking, StoredPacking, 
		ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, External) ->
	{SourcePacking, UnpackKey} = chunk_key(StoredPacking, ChunkOffset, TXRoot),
	{TargetPacking, PackKey} = chunk_key(RequestedPacking, ChunkOffset, TXRoot),
	case validate_chunk_size(SourcePacking, Chunk, ChunkSize) of
		{ok, _} ->
			PrometheusLabel = atom_to_list(SourcePacking) ++ "_to_" ++ atom_to_list(TargetPacking),
			prometheus_histogram:observe_duration(packing_duration_milliseconds,
				[repack, PrometheusLabel, External], fun() ->
					ar_mine_randomx:randomx_reencrypt_chunk(SourcePacking, TargetPacking, 
						RandomXStateRef, UnpackKey, PackKey, Chunk, ChunkSize) end);
		Error ->
			Error
	end.

validate_chunk_size(spora_2_5, Chunk, ChunkSize) ->
	PackedSize = byte_size(Chunk),
	case PackedSize ==
			(((ChunkSize - 1) div (?DATA_CHUNK_SIZE)) + 1) * (?DATA_CHUNK_SIZE) of
		false ->
			{error, invalid_packed_size};
		true ->
			{ok, PackedSize}
	end;
validate_chunk_size(spora_2_6, Chunk, ChunkSize) ->
	PackedSize = byte_size(Chunk),
	case {PackedSize == ?DATA_CHUNK_SIZE, ChunkSize =< PackedSize andalso ChunkSize > 0} of
		{false, _} ->
			{error, invalid_packed_size};
		{true, false} ->
			%% In practice, we would never get here because the merkle proof
			%% validation does not allow ChunkSize to exceed ?DATA_CHUNK_SIZE.
			{error, invalid_chunk_size};
		_ ->
			{ok, PackedSize}
	end.

increment_buffer_size() ->
	ets:update_counter(?MODULE, buffer_size, {2, 1}, {buffer_size, 1}).

decrement_buffer_size() ->
	ets:update_counter(?MODULE, buffer_size, {2, -1}, {buffer_size, 0}).

%%%===================================================================
%%% Prometheus metrics
%%%===================================================================

record_buffer_size_metric() ->
	case ets:lookup(?MODULE, buffer_size) of
		[{_, Size}] ->
			prometheus_gauge:set(packing_buffer_size, Size);
		_ ->
			ok
	end.

get_packing_latency(PackingStateRef) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:hash(sha256, crypto:strong_rand_bytes(256)),
	Pack = [PackingStateRef, Key, Chunk],
	Unpack = [PackingStateRef, Key, Chunk, ?DATA_CHUNK_SIZE],
	%% Run each randomx routine Repetitions times and return the minimum runtime. We use
	%% minimum rather than average since it more closely approximates the fastest that this
	%% machine can do the calculation.
	Repetitions = 5,
	{minimum_run_time(ar_mine_randomx, randomx_encrypt_chunk, [spora_2_5 | Pack], Repetitions),
		minimum_run_time(ar_mine_randomx, randomx_encrypt_chunk, [spora_2_6 | Pack], Repetitions),
		minimum_run_time(ar_mine_randomx, randomx_decrypt_chunk, [spora_2_5 | Unpack], Repetitions),
		minimum_run_time(ar_mine_randomx, randomx_decrypt_chunk, [spora_2_6 | Unpack], Repetitions)}.

record_packing_benchmarks({TheoreticalMaxRate, ChosenRate, Schedulers,
		ActualRatePack_2_5, ActualRatePack_2_6, ActualRateUnpack_2_5,
		ActualRateUnpack_2_6}) ->
	prometheus_gauge:set(packing_latency_benchmark,
		[protocol, pack, spora_2_6], ?PACKING_LATENCY_MS),
	prometheus_gauge:set(packing_latency_benchmark,
		[protocol, unpack, spora_2_6], ?PACKING_LATENCY_MS),
	prometheus_gauge:set(packing_rate_benchmark,
		[protocol], TheoreticalMaxRate),
	prometheus_gauge:set(packing_rate_benchmark,
		[configured], ChosenRate),
	prometheus_gauge:set(packing_schedulers,
		Schedulers),
	prometheus_gauge:set(packing_latency_benchmark,
		[init, pack, spora_2_5], ActualRatePack_2_5),
	prometheus_gauge:set(packing_latency_benchmark,
		[init, pack, spora_2_6], ActualRatePack_2_6),
	prometheus_gauge:set(packing_latency_benchmark,
		[init, unpack, spora_2_5], ActualRateUnpack_2_5),
	prometheus_gauge:set(packing_latency_benchmark,
		[init, unpack, spora_2_6], ActualRateUnpack_2_6).


minimum_run_time(Module, Function, Args, Repetitions) ->
	minimum_run_time(Module, Function, Args, Repetitions, infinity).
minimum_run_time(_Module, _Function, _Args, 0, MinTime) ->
	%% round microseconds to the nearest millisecond
	max(1, (MinTime + 500) div 1000);
minimum_run_time(Module, Function, Args, Repetitions, MinTime) ->
	{RunTime, _} = timer:tc(Module, Function, Args),
	minimum_run_time(Module, Function, Args, Repetitions-1, erlang:min(MinTime, RunTime)).

%% @doc Walk up the stack trace to the parent of the current function. E.g.
%% example() ->
%%     get_caller().
%%
%% Will return the caller of example/0.
get_caller() ->
    {current_stacktrace, CallStack} = process_info(self(), current_stacktrace),
    calling_function(CallStack).
calling_function([_, {_, _, _, _}|[{Module, Function, Arity, _}|_]]) ->
	atom_to_list(Module) ++ ":" ++ atom_to_list(Function) ++ "/" ++ integer_to_list(Arity);
calling_function(_) ->
    "unknown".

record_packing_request(Type, Packing, From) when
	is_atom(Type), is_atom(Packing), is_list(From) ->
	prometheus_counter:inc(
		packing_requests,
		[atom_to_list(Type), atom_to_list(Packing), From]);
record_packing_request(Type, Packing, From) when
	is_atom(From) ->
	record_packing_request(Type, Packing, atom_to_list(From));
record_packing_request(Type, Packing, From) when
	not is_atom(Packing) ->
	record_packing_request(Type, packing_atom(Packing), From).


%%%===================================================================
%%% Tests.
%%%===================================================================

pack_test() ->
	Root = crypto:strong_rand_bytes(32),
	Cases = [
		{<<1>>, 1, Root},
		{<<1>>, 2, Root},
		{<<0>>, 1, crypto:strong_rand_bytes(32)},
		{<<0>>, 2, crypto:strong_rand_bytes(32)},
		{<<0>>, 1234234534535, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(2), 234134234, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(3), 333, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(15), 9999999999999999999999999999,
				crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(16), 16, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024), 100000000000000, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024 - 1), 100000000000000,
				crypto:strong_rand_bytes(32)}
	],
	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	RandomXState = ar_mine_randomx:init_fast(<<1>>, Schedulers),
	PackedList = lists:flatten(lists:map(
		fun({Chunk, Offset, TXRoot}) ->
			ECDSA = ar_wallet:to_address(ar_wallet:new({ecdsa, secp256k1})),
			EDDSA = ar_wallet:to_address(ar_wallet:new({eddsa, ed25519})),
			{ok, Chunk, already_packed} = pack(unpacked, Offset, TXRoot, Chunk,
					RandomXState, external),
			{ok, Packed, was_not_already_packed} = pack(spora_2_5, Offset, TXRoot, Chunk,
					RandomXState, external),
			{ok, Packed2, was_not_already_packed} = pack({spora_2_6, ECDSA}, Offset, TXRoot,
					Chunk, RandomXState, external),
			{ok, Packed3, was_not_already_packed} = pack({spora_2_6, EDDSA}, Offset, TXRoot,
					Chunk, RandomXState, external),
			?assertNotEqual(Packed, Chunk),
			?assertNotEqual(Packed2, Chunk),
			?assertNotEqual(Packed3, Chunk),
			?assertEqual({ok, Packed, already_unpacked},
					unpack(unpacked, Offset, TXRoot, Packed, byte_size(Chunk), RandomXState,
							internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack(spora_2_5, Offset, TXRoot, Packed, byte_size(Chunk), RandomXState,
							internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({spora_2_6, ECDSA}, Offset, TXRoot, Packed2, byte_size(Chunk),
							RandomXState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({spora_2_6, EDDSA}, Offset, TXRoot, Packed3, byte_size(Chunk),
							RandomXState, internal)),
			[Packed, Packed2, Packed3]
		end,
		Cases
	)),
	?assertEqual(length(PackedList), sets:size(sets:from_list(PackedList))).

calculate_throttle_delay_test() ->
	%% 1000 / ?PACKING_LATENCY_MS = 16.666666
	?assertEqual(0, calculate_throttle_delay(1, 17),
		"PackingRate > SpawnSchedulers capacity -> no throttle"),
	?assertEqual(0, calculate_throttle_delay(8, 1000),
		"PackingRate > SpawnSchedulers capacity -> no throttle"),
	?assertEqual(2, calculate_throttle_delay(1, 16),
		"PackingRate < SpawnSchedulers capacity -> throttle"),
	?assertEqual(15, calculate_throttle_delay(8, 100),
		"PackingRate < SpawnSchedulers capacity -> throttle"),
	?assertEqual(0, calculate_throttle_delay(0, 100),
		"0 schedulers -> no throttle"),
	?assertEqual(0, calculate_throttle_delay(8, 0),
		"no packing -> no throttle").