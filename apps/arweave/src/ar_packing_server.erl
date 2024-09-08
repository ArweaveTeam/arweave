-module(ar_packing_server).

-behaviour(gen_server).

-export([start_link/0, packing_atom/1, get_packing_state/0, get_randomx_state_by_difficulty/2,
		request_unpack/2, request_unpack/3, request_repack/2, request_repack/3,
		pack/4, unpack/5, repack/6, unpack_sub_chunk/5,
		is_buffer_full/0, record_buffer_size_metric/0,
		pad_chunk/1, unpad_chunk/3, unpad_chunk/4]).

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
packing_atom({spora_2_6, _Addr}) ->
	spora_2_6;
packing_atom({composite, _Addr, _Diff}) ->
	composite.

request_unpack(Ref, Args) ->
	request_unpack(Ref, self(), Args).

request_unpack(Ref, ReplyTo, Args) ->
	gen_server:cast(?MODULE, {unpack_request, ReplyTo, Ref, Args}).

request_repack(Ref, Args) ->
	request_repack(Ref, self(), Args).

request_repack(Ref, ReplyTo, Args) ->
	gen_server:cast(?MODULE, {repack_request, ReplyTo, Ref, Args}).

%% @doc Pack the chunk for mining. Packing ensures every mined chunk of data is globally
%% unique and cannot be easily inferred during mining from any metadata stored in RAM.
pack(Packing, ChunkOffset, TXRoot, Chunk) ->
	PackingState = get_packing_state(),
	record_packing_request(pack, Packing, unpacked, get_caller()),
	case pack(Packing, ChunkOffset, TXRoot, Chunk, PackingState, external) of
		{ok, Packed, _} ->
			{ok, Packed};
		Reply ->
			Reply
	end.

%% @doc Unpack the chunk packed for mining.
%%
%% Return {ok, UnpackedChunk} or {error, invalid_packed_size} or {error, invalid_chunk_size}
%% or {error, invalid_padding}.
unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
	PackingState = get_packing_state(),
	record_packing_request(unpack, unpacked, Packing, get_caller()),
	case unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, external) of
		{ok, Unpacked, _WasAlreadyUnpacked} ->
			{ok, Unpacked};
		Reply ->
			Reply
	end.

%% @doc Unpack the packed sub-chunk of a composite packing.
%%
%% Return {ok, UnpackedSubChunk} or {error, invalid_packed_size}.
unpack_sub_chunk(Packing, AbsoluteEndOffset, TXRoot, Chunk, SubChunkStartOffset) ->
	case byte_size(Chunk) == ?COMPOSITE_PACKING_SUB_CHUNK_SIZE of
		false ->
			{error, invalid_packed_size};
		true ->
			PackingState = get_packing_state(),
			record_packing_request(unpack_sub_chunk, not_set, Packing, get_caller()),
			{PackingAtom, Key} = chunk_key(Packing, AbsoluteEndOffset, TXRoot),
			RandomXState = get_randomx_state_by_packing(Packing, PackingState),
			case prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[unpack_sub_chunk, PackingAtom, external], fun() ->
						ar_mine_randomx:randomx_decrypt_sub_chunk(Packing, RandomXState,
									Key, Chunk, SubChunkStartOffset) end) of
				{ok, UnpackedSubChunk} ->
					{ok, UnpackedSubChunk};
				Error ->
					Error
			end
	end.

repack(RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
	PackingState = get_packing_state(),
	record_packing_request(repack, RequestedPacking, StoredPacking, get_caller()),
	repack(
		RequestedPacking, StoredPacking, ChunkOffset, TXRoot,
		Chunk, ChunkSize, PackingState, external).

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

pad_chunk(Chunk) ->
	pad_chunk(Chunk, byte_size(Chunk)).
pad_chunk(Chunk, ChunkSize) when ChunkSize == (?DATA_CHUNK_SIZE) ->
	Chunk;
pad_chunk(Chunk, ChunkSize) ->
	Zeros =
		case erlang:get(zero_chunk) of
			undefined ->
				ZeroChunk = << <<0>> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
				%% Cache the zero chunk in the process memory, constructing
				%% it is expensive.
				erlang:put(zero_chunk, ZeroChunk),
				ZeroChunk;
			ZeroChunk ->
				ZeroChunk
		end,
	PaddingSize = (?DATA_CHUNK_SIZE) - ChunkSize,
	<< Chunk/binary, (binary:part(Zeros, 0, PaddingSize))/binary >>.

unpad_chunk(spora_2_5, Unpacked, ChunkSize, _PackedSize) ->
	binary:part(Unpacked, 0, ChunkSize);
unpad_chunk({spora_2_6, _Addr}, Unpacked, ChunkSize, PackedSize) ->
	unpad_chunk(Unpacked, ChunkSize, PackedSize);
unpad_chunk({composite, _Addr, _PackingDifficulty}, Unpacked, ChunkSize, PackedSize) ->
	unpad_chunk(Unpacked, ChunkSize, PackedSize).

unpad_chunk(Unpacked, ChunkSize, PackedSize) ->
	Padding = binary:part(Unpacked, ChunkSize, PackedSize - ChunkSize),
	case Padding of
		<<>> ->
			Unpacked;
		_ ->
			case is_zero(Padding) of
				false ->
					error;
				true ->
					binary:part(Unpacked, 0, ChunkSize)
			end
	end.

is_zero(<< 0:8, Rest/binary >>) ->
	is_zero(Rest);
is_zero(<<>>) ->
	true;
is_zero(_Rest) ->
	false.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_packing_state() ->
	[{_, PackingState}] = ets:lookup(?MODULE, randomx_packing_state),
	PackingState.

get_randomx_state_by_difficulty(PackingDifficulty, PackingState) ->
	{RandomXState512, RandomXState4096} = PackingState,
	case PackingDifficulty of
		0 ->
			RandomXState512;
		_ ->
			RandomXState4096
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	
	ar:console("~nInitialising RandomX dataset for fast packing. Key: ~p. "
			"The process may take several minutes.~n", [ar_util:encode(?RANDOMX_PACKING_KEY)]),
	{RandomXState512, _RandomXState4096} = PackingState = init_packing_state(),
	ar:console("RandomX dataset initialisation complete.~n", []),
	{H0, H1} = ar_bench_hash:run_benchmark(RandomXState512),
	H0String = io_lib:format("~.3f", [H0 / 1000]),
	H1String = io_lib:format("~.3f", [H1 / 1000]),
	ar:console("Hashing benchmark~nH0: ~s ms~nH1/H2: ~s ms~n", [H0String, H1String]),
	?LOG_INFO([{event, hash_benchmark}, {h0_ms, H0String}, {h1_ms, H1String}]),
	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	{ActualRatePack2_6, ActualRatePackComposite} = get_packing_latency(PackingState),
	PackingLatency = ActualRatePackComposite,
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
	
	record_packing_benchmarks(TheoreticalMaxRate, PackingRate, Schedulers,
		ActualRatePack2_6, ActualRatePackComposite),
	SpawnSchedulers = min(SchedulersRequired, Schedulers),
	ar:console("~nStarting ~B packing threads.~n", [SpawnSchedulers]),
	%% Since the total rate of spawned processes might exceed the desired rate,
	%% artificially throttle processes uniformly.
	ThrottleDelay = calculate_throttle_delay(SpawnSchedulers, PackingRate),
	Workers = queue:from_list(
		[spawn_link(fun() -> worker(ThrottleDelay, PackingState) end)
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
	record_packing_request(unpack, unpacked, Packing, unpack_request),
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
			record_packing_request(pack, RequestedPacking, unpacked, repack_request),
			Worker ! {pack, Ref, From, {RequestedPacking, Chunk, AbsoluteOffset, TXRoot,
					ChunkSize}},
			{noreply, State#state{ workers = queue:in(Worker, Workers2) }};
		_ ->
			increment_buffer_size(),
			record_packing_request(repack, RequestedPacking, Packing, repack_request),
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
init_packing_state() ->
	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	RandomXState512 = ar_mine_randomx:init_fast(rx512, ?RANDOMX_PACKING_KEY, Schedulers),
	RandomXState4096 = ar_mine_randomx:init_fast(rx4096, ?RANDOMX_PACKING_KEY, Schedulers),
	PackingState = {RandomXState512, RandomXState4096},
	ets:insert(?MODULE, {randomx_packing_state, PackingState}),
	PackingState.

get_randomx_state_by_packing({composite, _, _}, {_RandomXState512, RandomXState4096}) ->
	RandomXState4096;
get_randomx_state_by_packing(_Packing, {RandomXState512, _RandomXState4096}) ->
	RandomXState512.

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

worker(ThrottleDelay, PackingState) ->
	receive
		{unpack, Ref, From, Args} ->
			{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case unpack(Packing, AbsoluteOffset, TXRoot, Chunk, ChunkSize,
					PackingState, internal) of
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
			worker(ThrottleDelay, PackingState);
		{pack, Ref, From, Args} ->
			{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case pack(Packing, AbsoluteOffset, TXRoot, Chunk, PackingState, internal) of
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
			worker(ThrottleDelay, PackingState);
		{repack, Ref, From, Args} ->
			{RequestedPacking, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case repack(RequestedPacking, Packing,
					AbsoluteOffset, TXRoot, Chunk, ChunkSize, PackingState, internal) of
				{ok, Packed, _RepackInput} ->
					From ! {chunk, {packed, Ref,
							{RequestedPacking, Packed, AbsoluteOffset, TXRoot, ChunkSize}}},
					case RequestedPacking == Packing of
						true ->
							%% When RequestdPacking and Packing are the same
							%% the repack does no work and just returns
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
			worker(ThrottleDelay, PackingState)
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
	};
chunk_key({composite, RewardAddr, PackingDiff}, ChunkOffset, TXRoot) ->
	%% This is only a part of the packing key. Each sub-chunk is packed using a different
	%% key composed from the key returned by this function and the relative sub-chunk offset.
	{
		composite,
		crypto:hash(sha256, << ChunkOffset:256, TXRoot:32/binary, PackingDiff:8,
				RewardAddr/binary >>)
	}.

pack(unpacked, _ChunkOffset, _TXRoot, Chunk, _PackingState, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_packed};
pack(Packing, ChunkOffset, TXRoot, Chunk, PackingState, External) ->
	case byte_size(Chunk) > ?DATA_CHUNK_SIZE of
		true ->
			{error, invalid_unpacked_size};
		false ->
			{PackingAtom, Key} = chunk_key(Packing, ChunkOffset, TXRoot),
			RandomXState = get_randomx_state_by_packing(Packing, PackingState),
			case prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[pack, PackingAtom, External], fun() ->
							ar_mine_randomx:randomx_encrypt_chunk(Packing, RandomXState,
									Key, Chunk) end) of
				{ok, Packed} ->
					{ok, Packed, was_not_already_packed};
				Error ->
					Error
			end
	end.

unpack(unpacked, _ChunkOffset, _TXRoot, Chunk, _ChunkSize, _PackingState, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_unpacked};
unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	case validate_chunk_size(Packing, Chunk, ChunkSize) of
		{error, Reason} ->
			{error, Reason};
		{ok, _PackedSize} ->
			{PackingAtom, Key} = chunk_key(Packing, ChunkOffset, TXRoot),
			RandomXState = get_randomx_state_by_packing(Packing, PackingState),
			case prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[unpack, PackingAtom, External], fun() ->
							ar_mine_randomx:randomx_decrypt_chunk(Packing, RandomXState,
									Key, Chunk, ChunkSize) end) of
				{ok, Unpacked} ->
					{ok, Unpacked, was_not_already_unpacked};
				Error ->
					Error
			end
	end.

repack(unpacked, unpacked,
		_ChunkOffset, _TXRoot, Chunk, _ChunkSize, _PackingState, _External) ->
	{ok, Chunk, Chunk};

repack(RequestedPacking, unpacked,
		ChunkOffset, TXRoot, Chunk, _ChunkSize, PackingState, External) ->
	case pack(RequestedPacking, ChunkOffset, TXRoot, Chunk, PackingState, External) of
		{ok, Packed, _WasAlreadyPacked} ->
			{ok, Packed, Chunk};
		Error ->
			Error
	end;

repack(unpacked, StoredPacking,
		ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	case unpack(StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) of
		{ok, Unpacked, _WasAlreadyUnpacked} ->
			{ok, Unpacked, Unpacked};
		Error ->
			Error
	end;

repack(RequestedPacking, StoredPacking,
		_ChunkOffset, _TXRoot, Chunk, _ChunkSize, _PackingState, _External)
		when StoredPacking == RequestedPacking ->
	%% StoredPacking and Packing are in the same format and neither is unpacked. To
	%% avoid uneccessary unpacking we'll return none for the UnpackedChunk. If a caller
	%% needs the UnpackedChunk they should call unpack explicity.
	{ok, Chunk, none};

repack({composite, RequestedAddr, RequestedPackingDifficulty} = RequestedPacking,
		{composite, StoredAddr, StoredPackingDifficulty} = StoredPacking,
			ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External)
		when RequestedAddr == StoredAddr,
			StoredPackingDifficulty > RequestedPackingDifficulty ->
	repack_no_nif({RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
			ChunkSize, PackingState, External});

repack({composite, _Addr, _PackingDifficulty} = RequestedPacking,
		{spora_2_6, _StoredAddr} = StoredPacking,
			ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	repack_no_nif({RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
			ChunkSize, PackingState, External});

repack({spora_2_6, _StoredAddr} = RequestedPacking,
		{composite, _Addr, _PackingDifficulty} = StoredPacking,
			ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	repack_no_nif({RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
			ChunkSize, PackingState, External});

repack({composite, _Addr, _PackingDifficulty} = RequestedPacking,
		spora_2_5 = StoredPacking,
			ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	repack_no_nif({RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
			ChunkSize, PackingState, External});

repack(RequestedPacking, StoredPacking,
		ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
	{SourcePackingAtom, UnpackKey} = chunk_key(StoredPacking, ChunkOffset, TXRoot),
	{TargetPackingAtom, PackKey} = chunk_key(RequestedPacking, ChunkOffset, TXRoot),
	case validate_chunk_size(StoredPacking, Chunk, ChunkSize) of
		{ok, _} ->
			PrometheusLabel = atom_to_list(SourcePackingAtom) ++ "_to_"
					++ atom_to_list(TargetPackingAtom),
			%% By the time we hit this branch both RequestedPacking and StoredPacking should
			%% use the same RandomX state (i.e. both are either spora_2_5/spora_2_6 or both
			%% composite).
			RandomXState = get_randomx_state_by_packing(RequestedPacking, PackingState),
			prometheus_histogram:observe_duration(packing_duration_milliseconds,
				[repack, PrometheusLabel, External], fun() ->
					ar_mine_randomx:randomx_reencrypt_chunk(StoredPacking, RequestedPacking,
							RandomXState, UnpackKey, PackKey, Chunk, ChunkSize) end);
		Error ->
			Error
	end.

repack_no_nif(Args) ->
	{RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
			ChunkSize, PackingState, External} = Args,
	case unpack(StoredPacking, ChunkOffset, TXRoot,
			Chunk, ChunkSize, PackingState, External) of
		{ok, Unpacked, _WasAlreadyUnpacked} ->
			case pack(RequestedPacking, ChunkOffset, TXRoot, Chunk, PackingState, External) of
				{ok, Packed, _WasAlreadyPacked} ->
					{ok, Packed, Unpacked};
				Error2 ->
					Error2
			end;
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
validate_chunk_size({spora_2_6, _Addr}, Chunk, ChunkSize) ->
	validate_chunk_size(Chunk, ChunkSize);
validate_chunk_size({composite, _Addr, _PackingDifficulty}, Chunk, ChunkSize) ->
	validate_chunk_size(Chunk, ChunkSize).

validate_chunk_size(Chunk, ChunkSize) ->
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

get_packing_latency(PackingState) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:hash(sha256, crypto:strong_rand_bytes(256)),
	Addr = crypto:strong_rand_bytes(32),
	Spora2_6Packing = {spora_2_6, Addr},
	CompositePacking = {composite, Addr, 1},
	Spora2_6RandomXState = get_randomx_state_by_packing(Spora2_6Packing, PackingState),
	CompositeRandomXState = get_randomx_state_by_packing(CompositePacking, PackingState),
	%% Run each randomx routine Repetitions times and return the minimum runtime. We use
	%% minimum rather than average since it more closely approximates the fastest that this
	%% machine can do the calculation.
	Repetitions = 5,
	{minimum_run_time(ar_mine_randomx, randomx_encrypt_chunk,
			[Spora2_6Packing, Spora2_6RandomXState, Key, Chunk], Repetitions),
		minimum_run_time(ar_mine_randomx, randomx_encrypt_chunk,
			[CompositePacking, CompositeRandomXState, Key, Chunk], Repetitions)}.

record_packing_benchmarks(TheoreticalMaxRate, ChosenRate, Schedulers,
ActualRatePack2_6, ActualRatePackComposite) ->
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
		[init, pack, spora_2_6], ActualRatePack2_6),
	prometheus_gauge:set(packing_latency_benchmark,
		[init, pack, composite], ActualRatePackComposite).

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

%% @doc Log actual packings and unpackings
%% where the StoredPacking does not match the RequestedPacking.
record_packing_request(_Type, RequestedPacking, StoredPacking, _From)
  		when RequestedPacking == StoredPacking ->
	ok;
record_packing_request(unpack, _RequestedPacking, StoredPacking, From) ->
	%% When unpacking we care about StoredPacking (i.e. what we're unpacking from).
	prometheus_counter:inc(
		packing_requests,
		[unpack, packing_atom(StoredPacking), From]);
record_packing_request(unpack_sub_chunk, _RequestedPacking, StoredPacking, From) ->
	%% When unpacking we care about StoredPacking (i.e. what we're unpacking from).
	prometheus_counter:inc(
		packing_requests,
		[unpack_sub_chunk, packing_atom(StoredPacking), From]);
record_packing_request(Type, RequestedPacking, _StoredPacking, From) ->
	%% Type is either `pack` or `unpack` in both cases we record RequestedPacking.
	prometheus_counter:inc(
		packing_requests,
		[Type, packing_atom(RequestedPacking), From]).

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
	PackingState = init_packing_state(),
	PackedList = lists:flatten(lists:map(
		fun({Chunk, Offset, TXRoot}) ->
			ECDSA = ar_wallet:to_address(ar_wallet:new({ecdsa, secp256k1})),
			EDDSA = ar_wallet:to_address(ar_wallet:new({eddsa, ed25519})),
			{ok, Chunk, already_packed} = pack(unpacked, Offset, TXRoot, Chunk,
						PackingState, external),
			{ok, Packed, was_not_already_packed} = pack(spora_2_5, Offset, TXRoot, Chunk,
						PackingState, external),
			{ok, Packed2, was_not_already_packed} = pack({spora_2_6, ECDSA}, Offset, TXRoot,
					Chunk, PackingState, external),
			{ok, Packed3, was_not_already_packed} = pack({spora_2_6, EDDSA}, Offset, TXRoot,
					Chunk, PackingState, external),
			{ok, Packed4, was_not_already_packed} = pack({composite, ECDSA, 1}, Offset, TXRoot,
					Chunk, PackingState, external),
			{ok, Packed5, was_not_already_packed} = pack({composite, EDDSA, 1}, Offset, TXRoot,
					Chunk, PackingState, external),
			{ok, Packed6, was_not_already_packed} = pack({composite, ECDSA, 2}, Offset, TXRoot,
					Chunk, PackingState, external),
			{ok, Packed7, was_not_already_packed} = pack({composite, EDDSA, 2}, Offset, TXRoot,
					Chunk, PackingState, external),
			?assertNotEqual(Packed, Chunk),
			?assertNotEqual(Packed2, Chunk),
			?assertNotEqual(Packed3, Chunk),
			?assertNotEqual(Packed4, Chunk),
			?assertNotEqual(Packed5, Chunk),
			?assertNotEqual(Packed6, Chunk),
			?assertNotEqual(Packed7, Chunk),
			?assertEqual({ok, Packed, already_unpacked},
					unpack(unpacked, Offset, TXRoot, Packed, byte_size(Chunk), PackingState,
							internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack(spora_2_5, Offset, TXRoot, Packed, byte_size(Chunk), PackingState,
							internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({spora_2_6, ECDSA}, Offset, TXRoot, Packed2, byte_size(Chunk),
							PackingState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({spora_2_6, EDDSA}, Offset, TXRoot, Packed3, byte_size(Chunk),
							PackingState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({composite, ECDSA, 1}, Offset, TXRoot, Packed4, byte_size(Chunk),
							PackingState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({composite, EDDSA, 1}, Offset, TXRoot, Packed5, byte_size(Chunk),
							PackingState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({composite, ECDSA, 2}, Offset, TXRoot, Packed6, byte_size(Chunk),
							PackingState, internal)),
			?assertEqual({ok, Chunk, was_not_already_unpacked},
					unpack({composite, EDDSA, 2}, Offset, TXRoot, Packed7, byte_size(Chunk),
							PackingState, internal)),
			[Packed, Packed2, Packed3, Packed4, Packed5, Packed6, Packed7]
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
