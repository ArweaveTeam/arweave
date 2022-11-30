-module(ar_packing_server).

-behaviour(gen_server).

-export([start_link/0, pack/4, unpack/5, is_buffer_full/0, record_buffer_size_metric/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% The packing latency as it is chosen for the protocol.
-define(PACKING_LATENCY_MS, 300).

%% The key to initialize the RandomX state from, for RandomX packing.
-define(RANDOMX_PACKING_KEY, <<"default arweave 2.5 pack key">>).

%% The approximate maximum number of chunks to schedule for packing/unpacking.
%% A bigger number means more memory allocated for the chunks not packed/unpacked yet.
-define(PACKING_BUFFER_SIZE, 1000).

-record(state, {
	workers,
	repack_map = maps:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Pack the chunk for mining. Packing ensures every mined chunk of data is globally
%% unique and cannot be easily inferred during mining from any metadata stored in RAM.
pack(Packing, ChunkOffset, TXRoot, Chunk) ->
	[{_, RandomXStateRef}] = ets:lookup(?MODULE, randomx_packing_state),
	case pack(Packing, ChunkOffset, TXRoot, Chunk, RandomXStateRef, external) of
		{ok, Packed, _} ->
			{ok, Packed};
		Reply ->
			Reply
	end.

%% @doc Unpack the chunk packed for mining. Return {ok, UnpackedChunk} or
%% {error, invalid_packed_size}.
unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
	[{_, RandomXStateRef}] = ets:lookup(?MODULE, randomx_packing_state),
	case unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, external) of
		{ok, Unpacked, _} ->
			{ok, Unpacked};
		Reply ->
			Reply
	end.

%% @doc Return true if the packing server buffer is considered full, to apply
%% some back-pressure on the pack/4 and unpack/5 callers.
is_buffer_full() ->
	case ets:lookup(?MODULE, buffer_size) of
		[{_, Size}] when Size > ?PACKING_BUFFER_SIZE ->
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
	%% Cache 256 KiB of 0 bytes so that we can add right padding quickly.
	ZeroChunk = << <<0>> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
	erlang:put(zero_chunk, ZeroChunk),
	ets:insert(?MODULE, {zero_chunk, ZeroChunk}),
	{ok, Config} = application:get_env(arweave, config),
	PackingRate = Config#config.packing_rate,
	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	SchedulersRequired = ceil(PackingRate / (1000 / (?PACKING_LATENCY_MS))),
	case SchedulersRequired > Schedulers of
		true ->
			log_insufficient_core_count(Schedulers, PackingRate);
		false ->
			ok
	end,
	ar:console("Initialising RandomX dataset for fast packing. Key: ~p. "
			"The process may take several minutes.~n", [ar_util:encode(?RANDOMX_PACKING_KEY)]),
	PackingStateRef = ar_mine_randomx:init_fast(?RANDOMX_PACKING_KEY, Schedulers),
	ets:insert(?MODULE, {randomx_packing_state, PackingStateRef}),
	ar:console("RandomX dataset initialisation complete.~n", []),
	SpawnSchedulers = min(SchedulersRequired, Schedulers),
	%% Since the total rate of spawned processes might exceed the desired rate,
	%% artificially throttle processes uniformly.
	ThrottleDelay = calculate_throttle_delay(SpawnSchedulers, PackingRate),
	Workers = queue:from_list(
		[spawn_link(fun() -> worker(ThrottleDelay, PackingStateRef) end)
			|| _ <- lists:seq(1, SpawnSchedulers)]),
	ok = ar_events:subscribe(chunk),
	ets:insert(?MODULE, {buffer_size, 0}),
	timer:apply_interval(200, ?MODULE, record_buffer_size_metric, []),
	{ok, #state{ workers = Workers }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, chunk, {unpack_request, Ref, Args}}, State) ->
	#state{ workers = Workers } = State,
	{{value, Worker}, Workers2} = queue:out(Workers),
	?LOG_DEBUG([{event, got_unpack_request}, {ref, Ref}]),
	increment_buffer_size(),
	Worker ! {unpack, Ref, self(), Args},
	{noreply, State#state{ workers = queue:in(Worker, Workers2) }};

handle_info({event, chunk, {repack_request, Ref, Args}}, State) ->
	#state{ workers = Workers, repack_map = RepackMap } = State,
	{RequestedPacking, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
	{{value, Worker}, Workers2} = queue:out(Workers),
	?LOG_DEBUG([{event, got_pack_request}, {ref, Ref}]),
	case {Packing, RequestedPacking} of
		{unpacked, unpacked} ->
			?LOG_DEBUG([{event, got_pack_request_already_unpacked}, {ref, Ref}]),
			ar_events:send(chunk, {packed, Ref, {unpacked, Chunk, AbsoluteOffset, TXRoot,
					ChunkSize}}),
			{noreply, State};
		{unpacked, _} ->
			?LOG_DEBUG([{event, sending_for_packing}, {ref, Ref}]),
			increment_buffer_size(),
			Worker ! {pack, Ref, self(), {RequestedPacking, Chunk, AbsoluteOffset, TXRoot,
					ChunkSize}},
			{noreply, State#state{ workers = queue:in(Worker, Workers2) }};
		_ ->
			?LOG_DEBUG([{event, got_pack_request_need_unpacking_first}, {ref, Ref}]),
			increment_buffer_size(),
			Worker ! {unpack, Ref, self(), {Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}},
			%% At first we have to unpack so we have to remember the
			%% reference for now, to not forget to pack later.
			{noreply, State#state{ repack_map = maps:put(Ref, RequestedPacking, RepackMap),
				workers = queue:in(Worker, Workers2) }}
	end;

handle_info({event, chunk, _}, State) ->
	{noreply, State};

handle_info({worker, {unpacked, Ref, Args}}, State) ->
	#state{ workers = Workers, repack_map = RepackMap } = State,
	{{value, Worker}, Workers2} = queue:out(Workers),
	case maps:get(Ref, RepackMap, not_found) of
		not_found ->
			?LOG_DEBUG([{event, worker_unpacked_chunk}, {ref, Ref}]),
			ar_events:send(chunk, {unpacked, Ref, Args}),
			{noreply, State};
		RequestedPacking ->
			?LOG_DEBUG([{event, worker_unpacked_chunk_for_packing}, {ref, Ref}]),
			{_Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			increment_buffer_size(),
			Worker ! {pack, Ref, self(),
				{RequestedPacking, Chunk, AbsoluteOffset, TXRoot, ChunkSize}},
			{noreply, State#state{ workers = queue:in(Worker, Workers2),
				repack_map = maps:remove(Ref, RepackMap) }}
	end;

handle_info({worker, {packed, Ref, Args}}, State) ->
	ar_events:send(chunk, {packed, Ref, Args}),
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

log_insufficient_core_count(Schedulers, PackingRate) ->
	ar:console("The number of cores on your machine (~B) is not sufficient for
		packing ~B chunks per second.~n", [Schedulers, PackingRate]),
	?LOG_WARNING([{event, insufficient_core_count_to_sustain_desired_packing_rate},
			{cores, Schedulers}, {packing_rate, PackingRate}]).

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
					From ! {worker, {unpacked, Ref, {Packing, U, AbsoluteOffset, TXRoot,
							ChunkSize}}},
					case AlreadyUnpacked of
						already_unpacked ->
							ok;
						_ ->
							timer:sleep(ThrottleDelay)
					end;
				{error, invalid_packed_size} ->
					?LOG_WARNING([{event, got_packed_chunk_of_invalid_size}])
			end,
			decrement_buffer_size(),
			worker(ThrottleDelay, RandomXStateRef);
		{pack, Ref, From, Args} ->
			{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = Args,
			case pack(Packing, AbsoluteOffset, TXRoot, Chunk, RandomXStateRef, internal) of
				{ok, P, AlreadyPacked} ->
					From ! {worker, {packed, Ref, {Packing, P, AbsoluteOffset, TXRoot,
							ChunkSize}}},
					case AlreadyPacked of
						already_packed ->
							ok;
						_ ->
							timer:sleep(ThrottleDelay)
					end;
				{error, invalid_unpacked_size} ->
					?LOG_WARNING([{event, got_packed_chunk_of_invalid_size}])
			end,
			decrement_buffer_size(),
			worker(ThrottleDelay, RandomXStateRef)
	end.

pack(unpacked, _ChunkOffset, _TXRoot, Chunk, _RandomXStateRef, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_packed};
pack(spora_2_5, ChunkOffset, TXRoot, Chunk, RandomXStateRef, External) ->
	case byte_size(Chunk) > ?DATA_CHUNK_SIZE of
		true ->
			{error, invalid_unpacked_size};
		false ->
			%% The presence of the absolute end offset in the key makes sure
			%% packing of every chunk is unique, even when the same chunk is
			%% present in the same transaction or across multiple transactions
			%% or blocks. The presence of the transaction root in the key
			%% ensures one cannot find data that has certain patterns after
			%% packing.
			Key = crypto:hash(sha256, << ChunkOffset:256, TXRoot/binary >>),
			{ok, prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[pack, External], fun() ->
							ar_mine_randomx:randomx_encrypt_chunk(RandomXStateRef, Key,
									pad_chunk(Chunk)) end), was_not_already_packed}
	end;
pack({spora_2_6, RewardAddr}, ChunkOffset, TXRoot, Chunk, RandomXStateRef, External) ->
	case byte_size(Chunk) > ?DATA_CHUNK_SIZE of
		true ->
			{error, invalid_unpacked_size};
		false ->
			%% The presence of the absolute end offset in the key makes sure
			%% packing of every chunk is unique, even when the same chunk is
			%% present in the same transaction or across multiple transactions
			%% or blocks. The presence of the transaction root in the key
			%% ensures one cannot find data that has certain patterns after
			%% packing. The presence of the reward address, combined with
			%% the 2.6 mining mechanics, puts a relatively low cap on the performance
			%% of a single dataset replica, essentially incentivizing miners to create
			%% more weave replicas per invested dollar.
			Key = crypto:hash(sha256, << ChunkOffset:256, TXRoot:32/binary, RewardAddr/binary >>),
			{ok, prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[pack, External], fun() ->
							ar_mine_randomx:randomx_encrypt_chunk_2_6(RandomXStateRef, Key,
									pad_chunk(Chunk)) end), was_not_already_packed}
	end.

pad_chunk(Chunk) ->
	pad_chunk(Chunk, byte_size(Chunk)).

pad_chunk(Chunk, ChunkSize) when ChunkSize == (?DATA_CHUNK_SIZE) ->
	Chunk;
pad_chunk(Chunk, ChunkSize) ->
	ZeroChunk =
		case erlang:get(zero_chunk) of
			undefined ->
				[{_, C}] = ets:lookup(?MODULE, zero_chunk),
				C;
			C ->
				C
		end,
	PaddingSize = (?DATA_CHUNK_SIZE) - ChunkSize,
	<< Chunk/binary, (binary:part(ZeroChunk, 0, PaddingSize))/binary >>.

unpack(unpacked, _ChunkOffset, _TXRoot, Chunk, _ChunkSize, _RandomXStateRef, _External) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	{ok, Chunk, already_unpacked};
unpack(spora_2_5, ChunkOffset, TXRoot, Chunk, ChunkSize, RandomXStateRef, External) ->
	PackedSize = byte_size(Chunk),
	case PackedSize ==
			(((ChunkSize - 1) div (?DATA_CHUNK_SIZE)) + 1) * (?DATA_CHUNK_SIZE) of
		false ->
			{error, invalid_packed_size};
		true ->
			Key = crypto:hash(sha256, << ChunkOffset:256, TXRoot/binary >>),
			Unpacked = prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[unpack, External], fun() ->
							ar_mine_randomx:randomx_decrypt_chunk(RandomXStateRef, Key, Chunk,
									ChunkSize) end),
			{ok, binary:part(Unpacked, 0, ChunkSize), was_not_already_unpacked}
	end;
unpack({spora_2_6, RewardAddr}, ChunkOffset, TXRoot, Chunk, ChunkSize,
		RandomXStateRef, External) ->
	PackedSize = byte_size(Chunk),
	case PackedSize ==
			(((ChunkSize - 1) div (?DATA_CHUNK_SIZE)) + 1) * (?DATA_CHUNK_SIZE) of
		false ->
			{error, invalid_packed_size};
		true ->
			Key = crypto:hash(sha256, << ChunkOffset:256, TXRoot:32/binary, RewardAddr/binary >>),
			Unpacked = prometheus_histogram:observe_duration(packing_duration_milliseconds,
					[unpack, External], fun() ->
							ar_mine_randomx:randomx_decrypt_chunk_2_6(RandomXStateRef, Key, Chunk,
									ChunkSize) end),
			{ok, binary:part(Unpacked, 0, ChunkSize), was_not_already_unpacked}
	end.

increment_buffer_size() ->
	ets:update_counter(?MODULE, buffer_size, {2, 1}, {buffer_size, 1}).

decrement_buffer_size() ->
	ets:update_counter(?MODULE, buffer_size, {2, -1}, {buffer_size, 0}).

record_buffer_size_metric() ->
	case ets:lookup(?MODULE, buffer_size) of
		[{_, Size}] ->
			prometheus_gauge:set(packing_buffer_size, Size);
		_ ->
			ok
	end.

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
