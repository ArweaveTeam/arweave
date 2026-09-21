-module(ar_packing_server).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, packing_atom/1, get_packing_state/0, get_randomx_state_for_h0/2,
         request_unpack/2, request_unpack/3, request_unpack/4,
         request_repack/2, request_repack/3, request_repack/4,
         request_encipher/3, request_decipher/3,
         request_encipher/4, request_decipher/4,
         pack/4, unpack/5, repack/6, unpack_sub_chunk/5,
         record_pending_requests_metric/0,
         pad_chunk/1, unpad_chunk/3, unpad_chunk/4,
         encipher_replica_2_9_chunk/2, decipher_replica_2_9_chunk/2,
         exor_replica_2_9_chunk/2, pack_replica_2_9_chunk/3,
         request_entropy_generation/3, request_entropy_slice/3]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

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
packing_atom({replica_2_9, _Addr}) ->
    replica_2_9.

request_unpack(Ref, Args) ->
    request_unpack(Ref, self(), Args).

request_unpack(Ref, ReplyTo, Args) ->
    submit(unpack, Ref, ReplyTo, Args).

request_unpack(Ref, ReplyTo, Args, CacheRef) ->
    submit(unpack, Ref, ReplyTo, Args, CacheRef).

request_repack(Ref, Args) ->
    request_repack(Ref, self(), Args).

request_repack(Ref, ReplyTo, Args) ->
    submit(repack, Ref, ReplyTo, Args).

request_repack(Ref, ReplyTo, Args, CacheRef) ->
    submit(repack, Ref, ReplyTo, Args, CacheRef).

request_encipher(Ref, ReplyTo, Args) ->
    submit(encipher, Ref, ReplyTo, Args).

request_encipher(Ref, ReplyTo, Args, CacheRef) ->
    submit(encipher, Ref, ReplyTo, Args, CacheRef).

request_decipher(Ref, ReplyTo, Args) ->
    submit(decipher, Ref, ReplyTo, Args).

request_decipher(Ref, ReplyTo, Args, CacheRef) ->
    submit(decipher, Ref, ReplyTo, Args, CacheRef).

request_entropy_generation(
  Ref, ReplyTo, {RewardAddr, BucketEndOffset, SubChunkStart, CacheEntropy}) ->
    gen_server:cast(?MODULE,
                    {generate_entropy, ReplyTo, Ref,
                     {RewardAddr, BucketEndOffset, SubChunkStart, CacheEntropy}}).

%% @doc Ask a packing worker for one chunk's slice of a cached entropy.
request_entropy_slice(Ref, ReplyTo, {RewardAddr, BucketEndOffset, SubChunkStart}) ->
    gen_server:cast(?MODULE,
                    {generate_entropy_slice, ReplyTo, Ref,
                     {RewardAddr, BucketEndOffset, SubChunkStart}}).

%% @doc Pack the chunk for mining. Packing ensures every mined chunk of data is globally
%% unique and cannot be easily inferred during mining from any metadata stored in RAM.
pack(Packing, ChunkOffset, TXRoot, Chunk) ->
    PackingState = get_packing_state(),
    record_packing_request(pack, Packing, unpacked),
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
    record_packing_request(unpack, unpacked, Packing),
    case unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, external) of
        {ok, Unpacked, _WasAlreadyUnpacked} ->
            {ok, Unpacked};
        Reply ->
            Reply
    end.

%% @doc Unpack the packed sub-chunk of a shared entropy replica.
%%
%% Return {ok, UnpackedSubChunk} or {error, invalid_packed_size}.
unpack_sub_chunk({replica_2_9, RewardAddr} = Packing,
                 AbsoluteEndOffset, _TXRoot, Chunk, SubChunkStartOffset) ->
    case byte_size(Chunk) == ?SUB_CHUNK_SIZE of
        false ->
            {error, invalid_packed_size};
        true ->
            record_packing_request(unpack_sub_chunk, not_set, Packing),
            Slice = arweave_entropy:generate_slice(
                        RewardAddr, AbsoluteEndOffset, SubChunkStartOffset),
            UnpackedSubChunk = prometheus_histogram:observe_duration(
                packing_duration_milliseconds,
                [unpack_sub_chunk, replica_2_9, external],
                fun() -> ar_mine_randomx:exor_sub_chunk(Chunk, Slice) end),
            {ok, UnpackedSubChunk}
    end.

repack(RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize) ->
    PackingState = get_packing_state(),
    record_packing_request(repack, RequestedPacking, StoredPacking),
    repack(
      RequestedPacking, StoredPacking, ChunkOffset, TXRoot,
      Chunk, ChunkSize, PackingState, external).

%% @doc Admit a standalone request whose caller has no pipeline reservation.
submit(Type, Ref, ReplyTo, Args) ->
    case ar_chunk_cache:reserve(packing) of
        full -> busy;
        {ok, CacheRef} ->
            try
                ar_chunk_cache:mark_cached(CacheRef),
                submit(Type, Ref, ReplyTo, Args, CacheRef)
            after ar_chunk_cache:release(CacheRef) end
    end.

%% @doc Add a cache reference before a payload enters the packing mailbox.
submit(Type, Ref, ReplyTo, Args, CacheRef) ->
    case whereis(?MODULE) of
        undefined -> busy;
        PID -> do_submit(PID, Type, Ref, ReplyTo, Args, CacheRef)
    end.

do_submit(PID, Type, Ref, ReplyTo, Args, CacheRef) ->
    case ar_chunk_cache:add_reference(CacheRef, PID) of
        {ok, PackingCacheRef} ->
            arweave_util:cast_after(
                600000, ReplyTo, {expire, Type, Ref}
            ),
            gen_server:cast(PID,
                {reserved, PackingCacheRef, Type, Ref, ReplyTo, Args});
        {error, expired} -> {error, cancelled}
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
unpad_chunk({replica_2_9, _Addr}, Unpacked, ChunkSize, PackedSize) ->
    unpad_chunk(Unpacked, ChunkSize, PackedSize);
unpad_chunk(unpacked, Unpacked, ChunkSize, _PackedSize) ->
    binary:part(Unpacked, 0, ChunkSize).

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

get_randomx_state_for_h0(PackingDifficulty, PackingState) ->
    {RandomXState512, RandomXState4096, _} = PackingState,
    case PackingDifficulty of
        0 ->
            RandomXState512;
        _ ->
            RandomXState4096
    end.

%% @doc Encipher the given chunk with the given 2.9 entropy assembled for this chunk.
%% Encipher and decipher are the same operation, only difference is how we record the operation.
-spec encipher_replica_2_9_chunk(
        Chunk :: binary(),
        Entropy :: binary()
       ) -> binary().
encipher_replica_2_9_chunk(Chunk, Entropy) ->
    record_packing_request(encipher, {replica_2_9, <<>>}, unpacked_padded),
    exor_replica_2_9_chunk(Chunk, Entropy).

%% @doc Decipher the given chunk with the given 2.9 entropy assembled for this chunk.
%% Encipher and decipher are the same operation, only difference is how we record the operation.
-spec decipher_replica_2_9_chunk(
        Chunk :: binary(),
        Entropy :: binary()
       ) -> binary().
decipher_replica_2_9_chunk(Chunk, Entropy) ->
    record_packing_request(decipher, unpacked_padded, {replica_2_9, <<>>}),
    exor_replica_2_9_chunk(Chunk, Entropy).

%% @doc Pad (to ?DATA_CHUNK_SIZE) and pack the chunk according to the 2.9 replication format.
%% Return the chunk and the combined entropy used on that chunk.
pack_replica_2_9_chunk(RewardAddr, AbsoluteEndOffset, Chunk) ->
    PaddedChunk = pad_chunk(Chunk),
    SubChunks = get_sub_chunks(PaddedChunk),
    pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, SubChunks).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
    ar:console("~nInitialising RandomX datasets. Keys: ~p, ~p. "
               "The process may take several minutes.~n",
               [arweave_util:encode(?RANDOMX_PACKING_KEY),
                arweave_util:encode(?RANDOMX_PACKING_KEY)]),
    {RandomXState512, _RandomXState4096, _RandomXStateSharedEntropy}
        = PackingState = init_packing_state(),
    ar:console("RandomX dataset initialisation complete.~n", []),
    {H0, H1} = ar_bench_hash:run_benchmark(RandomXState512),
    H0String = io_lib:format("~.3f", [H0 / 1000]),
    H1String = io_lib:format("~.3f", [H1 / 1000]),
    ar:console("Hashing benchmark~nH0: ~s ms~nH1/H2: ~s ms~n", [H0String, H1String]),
    ?LOG_INFO([{event, hash_benchmark}, {h0_ms, H0String}, {h1_ms, H1String}]),
    NumWorkers = arweave_config:get([packing, workers]),
    ar:console("~nStarting ~B packing threads.~n", [NumWorkers]),
    ?LOG_INFO([{event, starting_packing_threads}, {num_threads, NumWorkers}]),
    Workers = queue:from_list(
                [spawn_link(fun() -> worker(PackingState) end) || _ <- lists:seq(1, NumWorkers)]),
    ets:insert(?MODULE, {pending_requests, 0}),

    MaxSize = ar_chunk_cache:limit(),
    ar:console("~nShared chunk cache capacity: ~B chunks.~n", [MaxSize]),
    {ok, _} = ar_timer:apply_interval(
        200,
        ?MODULE,
        record_pending_requests_metric,
        [],
        #{skip_on_shutdown => false}
    ),
    {ok, #state{
            workers = Workers, num_workers = NumWorkers }}.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({reserved, CacheRef, Type, Ref, From, Args},
        #state{workers = Workers, num_workers = N} = State) when N > 0 ->
    {{value, Worker}, Workers2} = queue:out(Workers),
    case ar_chunk_cache:transfer(CacheRef, Worker) of
        {ok, WorkerCacheRef} ->
            increment_pending_requests(),
            Worker ! {reserved, WorkerCacheRef, Type, Ref, From, Args};
        {error, expired} -> ok
    end,
    {noreply, State#state{workers = queue:in(Worker, Workers2)}};
handle_cast({reserved, CacheRef, _Type, _Ref, _From, _Args}, State) ->
    ar_chunk_cache:release(CacheRef),
    {noreply, State};
handle_cast({generate_entropy, From, Ref, Args}, State) ->
    {noreply, send_to_worker({generate_entropy, Ref, From, Args}, State)};
handle_cast({generate_entropy_slice, From, Ref, Args}, State) ->
    {noreply, send_to_worker({generate_entropy_slice, Ref, From, Args}, State)};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
    ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

send_to_worker(Message, #state{ workers = Workers } = State) ->
    {{value, Worker}, Workers2} = queue:out(Workers),
    Worker ! Message,
    State#state{ workers = queue:in(Worker, Workers2) }.

init_packing_state() ->
    %% The RandomX datasets are derived solely from ?RANDOMX_PACKING_KEY
    %% (plus jit/large_pages), so they are identical on every start within a
    %% BEAM, and building them takes minutes. Cache the built state in
    %% persistent_term keyed by those inputs and reuse it across supervisor
    %% restarts. The datasets are read-only NIF resources terminate/2 never
    %% frees, so the cached ref stays valid after ar_packing_server stops.
    CacheKey = {?MODULE, randomx_packing_state, ?RANDOMX_PACKING_KEY,
                ar_mine_randomx:jit(), ar_mine_randomx:large_pages()},
    PackingState =
        case persistent_term:get(CacheKey, not_cached) of
            not_cached ->
                Built = build_packing_state(),
                persistent_term:put(CacheKey, Built),
                Built;
            Cached ->
                ?LOG_INFO([{event, reused_cached_randomx_packing_state}]),
                Cached
        end,
    ets:insert(?MODULE, {randomx_packing_state, PackingState}),
    PackingState.

build_packing_state() ->
    Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
    RandomXState512 = ar_mine_randomx:init_fast(rx512, ?RANDOMX_PACKING_KEY, Schedulers),
    RandomXState4096 = ar_mine_randomx:init_fast(rx4096, ?RANDOMX_PACKING_KEY, Schedulers),
    RandomXStateSharedEntropy = ar_mine_randomx:init_fast(rxsquared,
                                                          ?RANDOMX_PACKING_KEY, Schedulers),
    {RandomXState512, RandomXState4096, RandomXStateSharedEntropy}.

get_randomx_state_by_packing({replica_2_9, _}, {_, _, RandomXState}) ->
    RandomXState;
get_randomx_state_by_packing({spora_2_6, _}, {RandomXState, _, _}) ->
    RandomXState;
get_randomx_state_by_packing(spora_2_5, {RandomXState, _, _}) ->
    RandomXState.

worker(PackingState) ->
    receive
        {reserved, CacheRef, Type, Ref, From, Args} ->
            try
                process_reserved(Type, Ref, From, Args, PackingState, CacheRef)
            after
                decrement_pending_requests(),
                ar_chunk_cache:release(CacheRef)
            end,
            worker(PackingState);
        {generate_entropy, Ref, From, {RewardAddr, BucketEndOffset, SubChunkStart, CacheEntropy}} ->
            Entropy = arweave_entropy:generate(
                        RewardAddr, BucketEndOffset, SubChunkStart, CacheEntropy),
            From ! {entropy_generated, Ref, Entropy},
            worker(PackingState);
        {generate_entropy_slice, Ref, From, {RewardAddr, BucketEndOffset, SubChunkStart}} ->
            Slice = arweave_entropy:generate_slice(
                        RewardAddr, BucketEndOffset, SubChunkStart),
            From ! {entropy_generated, Ref, Slice},
            worker(PackingState)
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

pack(unpacked, _ChunkOffset, _TXRoot, Chunk, _PackingState, _External) ->
    %% Allows to reuse the same interface for unpacking and repacking.
    {ok, Chunk, already_packed};
pack(unpacked_padded, _ChunkOffset, _TXRoot, Chunk, _PackingState, _External) ->
    %% Allows to reuse the same interface for unpacking and repacking.
    {ok, pad_chunk(Chunk), was_not_already_packed};
pack({replica_2_9, RewardAddr}, AbsoluteEndOffset, _TXRoot, Chunk, _PackingState,
     _External) ->
    case byte_size(Chunk) > ?DATA_CHUNK_SIZE of
        true ->
            {error, invalid_unpacked_size};
        false ->
            PaddedChunk = pad_chunk(Chunk),
            SubChunks = get_sub_chunks(PaddedChunk),
            case pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset,
                                             SubChunks) of
                {ok, Packed, _Entropy} ->
                    {ok, Packed, was_not_already_packed};
                Error ->
                    Error
            end
    end;
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

get_sub_chunks(<< SubChunk:(?SUB_CHUNK_SIZE)/binary, Rest/binary >>) ->
    [SubChunk | get_sub_chunks(Rest)];
get_sub_chunks(<<>>) ->
    [].

pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, SubChunks) ->
    pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, 0, SubChunks, [], []).

pack_replica_2_9_sub_chunks(_RewardAddr, _AbsoluteEndOffset, _SubChunkStartOffset, [],
                            PackedSubChunks, EntropyParts) ->
    {ok, iolist_to_binary(lists:reverse(PackedSubChunks)),
     iolist_to_binary(lists:reverse(EntropyParts))};
pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, SubChunkStartOffset,
                            [SubChunk | SubChunks], PackedSubChunks, EntropyParts) ->
    Slice = arweave_entropy:generate_slice(RewardAddr, AbsoluteEndOffset, SubChunkStartOffset),
    PackedSubChunk = prometheus_histogram:observe_duration(
        packing_duration_milliseconds,
        [pack_sub_chunk, replica_2_9, internal],
        fun() -> ar_mine_randomx:exor_sub_chunk(SubChunk, Slice) end),
    pack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset,
                                SubChunkStartOffset + ?SUB_CHUNK_SIZE, SubChunks,
                                [PackedSubChunk | PackedSubChunks], [Slice | EntropyParts]).

unpack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, SubChunks) ->
    unpack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, 0, SubChunks, []).

unpack_replica_2_9_sub_chunks(_RewardAddr, _AbsoluteEndOffset, _SubChunkStartOffset, [],
                              UnpackedSubChunks) ->
    {ok, iolist_to_binary(lists:reverse(UnpackedSubChunks))};
unpack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset, SubChunkStartOffset,
                              [SubChunk | SubChunks], UnpackedSubChunks) ->
    Slice = arweave_entropy:generate_slice(RewardAddr, AbsoluteEndOffset, SubChunkStartOffset),
    UnpackedSubChunk = prometheus_histogram:observe_duration(
        packing_duration_milliseconds,
        [unpack_sub_chunk, replica_2_9, internal],
        fun() -> ar_mine_randomx:exor_sub_chunk(SubChunk, Slice) end),
    unpack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset,
                                  SubChunkStartOffset + ?SUB_CHUNK_SIZE, SubChunks,
                                  [UnpackedSubChunk | UnpackedSubChunks]).

unpack({replica_2_9, RewardAddr} = Packing, AbsoluteEndOffset,
       _TXRoot, Chunk, ChunkSize, _PackingState, _External) ->
    case validate_chunk_size(Packing, Chunk, ChunkSize) of
        {error, Reason} ->
            ?LOG_ERROR([{event, unpack_chunk_size_error}, {error, Reason},
                        {chunk_offset, AbsoluteEndOffset},
                        {packing, ar_serialize:encode_packing(Packing, true)},
                        {expected_chunk_size, ChunkSize},
                        {actual_chunk_size, byte_size(Chunk)}]),
            {error, Reason};
        {ok, PackedSize} ->
            SubChunks = get_sub_chunks(Chunk),
            case unpack_replica_2_9_sub_chunks(RewardAddr, AbsoluteEndOffset,
                                               SubChunks) of
                {ok, Unpacked} ->
                    case ar_packing_server:unpad_chunk(Packing, Unpacked,
                                                       ChunkSize, PackedSize) of
                        error ->
                            ?LOG_WARNING([{event, unpad_chunk_error},
                                          {packed_size, PackedSize},
                                          {chunk_size, ChunkSize},
                                          {absolute_end_offset, AbsoluteEndOffset}]),
                            {error, invalid_padding};
                        UnpackedChunk ->
                            {ok, UnpackedChunk, was_not_already_unpacked}
                    end;
                Error ->
                    ?LOG_ERROR([{event, unpack_replica_2_9_sub_chunks_error}, {error, Error}]),
                    Error
            end
    end;
unpack(unpacked, _ChunkOffset, _TXRoot, Chunk, _ChunkSize, _PackingState, _External) ->
    %% Allows to reuse the same interface for unpacking and repacking.
    {ok, Chunk, already_unpacked};
unpack(unpacked_padded, _ChunkOffset, _TXRoot, Chunk, ChunkSize, _PackingState, _External) ->
    {ok, binary:part(Chunk, 0, ChunkSize), was_not_already_unpacked};
unpack(Packing, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
    case validate_chunk_size(Packing, Chunk, ChunkSize) of
        {error, Reason} ->
            ?LOG_ERROR([{event, unpack_chunk_size_error}, {error, Reason},
                        {chunk_offset, ChunkOffset},
                        {packing, ar_serialize:encode_packing(Packing, true)},
                        {expected_chunk_size, ChunkSize},
                        {actual_chunk_size, byte_size(Chunk)}]),
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
    %% The difference with the next clause is that here we know the unpacked chunk
    %% and can explicitly return it as unpacked.
    {ok, Chunk, Chunk};
repack(RequestedPacking, StoredPacking,
       _ChunkOffset, _TXRoot, Chunk, _ChunkSize, _PackingState, _External)
  when StoredPacking == RequestedPacking ->
    %% StoredPacking and Packing are in the same format and neither is unpacked. To
    %% avoid uneccessary unpacking we'll return none for the UnpackedChunk. If a caller
    %% needs the UnpackedChunk they should call unpack explicity.
    {ok, Chunk, none};

repack(RequestedPacking, unpacked_padded,
       ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
    Unpacked = binary:part(Chunk, 0, ChunkSize),
    repack(RequestedPacking, unpacked,
           ChunkOffset, TXRoot, Unpacked, ChunkSize, PackingState, External);
repack(RequestedPacking, unpacked,
       ChunkOffset, TXRoot, Chunk, _ChunkSize, PackingState, External) ->
    case pack(RequestedPacking, ChunkOffset, TXRoot, Chunk, PackingState, External) of
        {ok, Packed, _WasAlreadyPacked} ->
            {ok, Packed, Chunk};
        Error ->
            Error
    end;

repack(unpacked_padded, StoredPacking,
       ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
    case unpack(StoredPacking, ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) of
        {ok, Unpacked, _WasAlreadyUnpacked} ->
            {ok, pad_chunk(Unpacked), Unpacked};
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

repack({replica_2_9, _} = RequestedPacking, StoredPacking,
       ChunkOffset, TXRoot, Chunk, ChunkSize, PackingState, External) ->
    repack_no_nif({RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
                   ChunkSize, PackingState, External});

repack(RequestedPacking, {replica_2_9, _} = StoredPacking,
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
            %% use the same RandomX state (i.e. both are either spora_2_5/spora_2_6).
            RandomXState = get_randomx_state_by_packing(RequestedPacking, PackingState),
            prometheus_histogram:observe_duration(packing_duration_milliseconds,
                                                  [repack, PrometheusLabel, External], fun() ->
                                                                                               ar_mine_randomx:randomx_reencrypt_chunk(StoredPacking, RequestedPacking,
                                                                                                                                       RandomXState, UnpackKey, PackKey, Chunk, ChunkSize) end);
        Error ->
            ?LOG_ERROR([{event, repack_chunk_size_error}, {error, Error},
                        {chunk_offset, ChunkOffset},
                        {requested_packing, ar_serialize:encode_packing(RequestedPacking, true)},
                        {stored_packing, ar_serialize:encode_packing(StoredPacking, true)},
                        {expected_chunk_size, ChunkSize},
                        {actual_chunk_size, byte_size(Chunk)}]),
            Error
    end.

repack_no_nif(Args) ->
    {RequestedPacking, StoredPacking, ChunkOffset, TXRoot, Chunk,
     ChunkSize, PackingState, External} = Args,
    case unpack(StoredPacking, ChunkOffset, TXRoot,
                Chunk, ChunkSize, PackingState, External) of
        {ok, Unpacked, _WasAlreadyUnpacked} ->
            case pack(RequestedPacking, ChunkOffset, TXRoot, Unpacked, PackingState, External) of
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
validate_chunk_size({replica_2_9, _Addr}, Chunk, ChunkSize) ->
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

increment_pending_requests() ->
    ets:update_counter(
        ?MODULE, pending_requests, {2, 1}, {pending_requests, 1}
    ).

decrement_pending_requests() ->
    ets:update_counter(
        ?MODULE, pending_requests, {2, -1}, {pending_requests, 0}
    ).

%%%===================================================================
%%% Prometheus metrics
%%%===================================================================

record_pending_requests_metric() ->
    case ets:lookup(?MODULE, pending_requests) of
        [{_, Size}] ->
            arweave_metrics:gauge_set(packing_buffer_size, Size);
        _ ->
            ok
    end.

%% @doc Log actual packings and unpackings
%% where the StoredPacking does not match the RequestedPacking.
record_packing_request(_Type, RequestedPacking, StoredPacking)
  when RequestedPacking == StoredPacking ->
    ok;
record_packing_request(Type, RequestedPacking, StoredPacking) ->
    Packing = case Type of
                  unpack -> StoredPacking;
                  unpack_sub_chunk -> StoredPacking;
                  decipher -> StoredPacking;
                  pack -> RequestedPacking;
                  repack -> RequestedPacking;
                  encipher -> RequestedPacking
              end,
    arweave_metrics:counter_inc(packing_requests, [Type, packing_atom(Packing)]).

exor_replica_2_9_chunk(Chunk, Entropy) ->
    iolist_to_binary(exor_replica_2_9_sub_chunks(Chunk, Entropy)).

exor_replica_2_9_sub_chunks(<<>>, <<>>) ->
    [];
exor_replica_2_9_sub_chunks(
  << SubChunk:(?SUB_CHUNK_SIZE)/binary, ChunkRest/binary >>,
  << EntropyPart:(?SUB_CHUNK_SIZE)/binary, EntropyRest/binary >>) ->
    [ar_mine_randomx:exor_sub_chunk(SubChunk, EntropyPart)
    | exor_replica_2_9_sub_chunks(ChunkRest, EntropyRest)].

%% @doc Process an admitted transformation without another memory check.
process_reserved(Type, Ref, From, Args, PackingState, CacheRef) ->
    Result = case {Type, Args} of
        {unpack, {Packing, Chunk, Offset, TXRoot, Size}} ->
            record_packing_request(unpack, unpacked, Packing),
            case unpack(Packing, Offset, TXRoot, Chunk, Size, PackingState,
                    internal) of
                {ok, Output, _} ->
                    {unpacked, Ref, {Packing, Output, Offset, TXRoot, Size}};
                {error, Reason} -> {unpack_error, Ref, Args, Reason};
                Error -> {unpack_error, Ref, Args, Error}
            end;
        {repack, {Target, Source, Chunk, Offset, TXRoot, Size}} ->
            RequestType = case Source of unpacked -> pack; _ -> repack end,
            record_packing_request(RequestType, Target, Source),
            case repack(Target, Source, Offset, TXRoot, Chunk, Size,
                    PackingState, internal) of
                {ok, Output, _} ->
                    {packed, Ref, {Target, Output, Offset, TXRoot, Size}};
                Error -> {repack_error, Ref, Error}
            end;
        {encipher, {Chunk, Entropy}} ->
            {enciphered, Ref, encipher_replica_2_9_chunk(Chunk, Entropy)};
        {decipher, {Chunk, Entropy}} ->
            {deciphered, Ref, decipher_replica_2_9_chunk(Chunk, Entropy)}
    end,
    case ar_chunk_cache:transfer(CacheRef, From) of
        {ok, ReplyCacheRef} -> From ! {chunk, Result, ReplyCacheRef};
        {error, expired} -> ok
    end,
    ok.

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
                                         ?assertNotEqual(Packed, Chunk),
                                         ?assertNotEqual(Packed2, Chunk),
                                         ?assertNotEqual(Packed3, Chunk),
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
                                         [Packed, Packed2, Packed3]
                                 end,
                                 Cases
                                )),
    ?assertEqual(length(PackedList), sets:size(sets:from_list(PackedList))).
