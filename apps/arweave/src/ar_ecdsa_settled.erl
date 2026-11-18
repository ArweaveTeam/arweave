%%% The ECDSA settled set: the identifiers of the ECDSA transactions from the
%%% on-chain blocks that left the block cache, each stored with the height of
%%% its block and the SHA-256 hash of its signature data segment, which covers
%%% every signed field of the header. Transaction validation reads the set at
%%% the height of the parent block, so the set must never skip a block. Disk
%%% errors are retried forever.
%%% A stored state record always describes the stored entries: an update
%%% writes both in one batch, and a rebuild removes the state record first and
%%% writes it last.
-module(ar_ecdsa_settled).
-test_category([fast]).

-export([initialize/3, settle/1, is_settled/2, has_tx/2, signature_data_hash/1,
        get_state/0, get_chunk/1, serialize_chunk/1, parse_chunk/1, copy/4]).

-include("ar.hrl").
-include("ar_ecdsa_settled.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DB, ecdsa_settled_2_db).

-define(DOWNLOAD_DB, ecdsa_settled_2_download_db).

%% The key of the {Height, BH, Count, XOR} state record, where BH is the hash of
%% the block at Height. The empty key sorts before every transaction identifier.
-define(STATE_KEY, <<>>).

%% A key bigger than every 32-byte transaction identifier.
-define(KEY_UPPER_BOUND, <<-1:256, 0:8>>).

-define(GENESIS_FILE, "genesis_data/ecdsa_settled").

%% The genesis file lists the ECDSA transactions settled at or below this block.
-define(GENESIS_HEIGHT, 2035370).
-define(GENESIS_BH,
        <<"m48_HpcMAAnPLLWfirS0OakYeAQ9_L_pp9wn-vf59UB1q-P03ZwtEmaGJ_CnpZnX">>).

-define(RETRY_INTERVAL_MS, 1000).

%% The number of entries copy/4 reads at a time.
-define(COPY_BATCH_SIZE, 1000).

%% The number of failed attempts after which a failure is logged as critical.
-define(FAILURES_BEFORE_CRITICAL, 3).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Settle every on-chain block up to the consensus window below the tip
%% of BI. Without a stored set ending at a block of BI or at a descendant of
%% its tip, or with one older than the set of Source, take the set from
%% Source: the trusted peers or the genesis file (peers), the snapshot
%% directory Dir ({snapshot, Dir}), or nowhere (local), returning
%% {error, ecdsa_settled_set_not_found} when it has none. Return
%% {error, {ecdsa_settled_peer_set_mismatch, Peer, Peer2, Height}} if the set
%% of the trusted peer Peer disagrees with the set Peer2 reports at Height.
%% Blocks are the recent blocks with their transaction headers.
initialize(BI, Blocks, Source) ->
    %% Finish a replacement a crash interrupted.
    promote_download(BI),
    Target = target(length(BI) - 1),
    case find_set(BI, Blocks, Target, Source) of
        ok ->
            truncate(Target, BI),
            settle_to(Target, BI, Blocks),
            %% A start may leave the stored state as it is.
            record_checksum(read_state()),
            ok;
        Error ->
            Error
    end.

%% @doc Add the ECDSA transactions of the given on-chain blocks above the
%% settled height to the set, writing each block atomically together with the
%% new state.
settle(Blocks) ->
    Sorted = lists:sort(
        fun(#block{ height = H1 }, #block{ height = H2 }) -> H1 =< H2 end,
        Blocks),
    lists:foreach(fun settle_block/1, Sorted).

%% @doc Return true if the given ECDSA transaction identifier was settled at
%% or below the given height.
is_settled(TXID, Height) ->
    case lookup(TXID) of
        not_found ->
            false;
        {SettledHeight, _Hash} ->
            SettledHeight =< Height
    end.

%% @doc Return true if the set holds the ECDSA transaction TX settled at Height
%% with the signature data of TX, false if it does not, or not_settled if the
%% set does not reach Height yet.
has_tx(#tx{ id = TXID } = TX, Height) ->
    case read_state() of
        {SettledHeight, _BH, _Count, _XOR} when Height =< SettledHeight ->
            lookup(TXID) == {Height, signature_data_hash(TX)};
        _ ->
            not_settled
    end.

%% @doc Return the SHA-256 hash of the signature data segment of TX.
signature_data_hash(TX) ->
    crypto:hash(sha256, ar_tx:generate_signature_data_segment(TX)).

%% @doc Return {ok, {Height, BH, Count, XOR}}, where BH is the hash of the
%% block at Height and Count and XOR describe the set of the transactions
%% settled at or below Height.
get_state() ->
    case ar_kv:get(?DB, ?STATE_KEY) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        Reply ->
            Reply
    end.

%% @doc Return {ok, Entries} with up to ?ECDSA_SETTLED_CHUNK_SIZE
%% {TXID, Height, Hash} entries with identifiers bigger than Cursor, in
%% ascending order.
get_chunk(<< _:32/binary >> = Cursor) ->
    case ar_kv:get_next_n(?DB, Cursor, ?ECDSA_SETTLED_CHUNK_SIZE + 1) of
        {ok, Pairs} ->
            Entries = entries(Pairs, Cursor),
            {ok, lists:sublist(Entries, ?ECDSA_SETTLED_CHUNK_SIZE)};
        Error ->
            Error
    end.

serialize_chunk(Entries) ->
    << << TXID/binary, Height:64, Hash/binary >>
            || {TXID, Height, Hash} <- Entries >>.

%% @doc Return {ok, Entries} if the binary is a chunk of strictly ascending
%% identifiers, or {error, invalid_chunk}.
parse_chunk(Bin) when byte_size(Bin) rem ?ECDSA_SETTLED_ENTRY_SIZE == 0,
        byte_size(Bin) =< ?MAX_SERIALIZED_ECDSA_SETTLED_CHUNK_SIZE ->
    Entries = [{TXID, Height, Hash}
            || << TXID:32/binary, Height:64, Hash:32/binary >> <= Bin],
    TXIDs = [TXID || {TXID, _, _} <- Entries],
    case TXIDs == lists:usort(TXIDs) of
        true ->
            {ok, Entries};
        false ->
            {error, invalid_chunk}
    end;
parse_chunk(_Bin) ->
    {error, invalid_chunk}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

lookup(TXID) ->
    Get = fun() -> ar_kv:get(?DB, TXID) end,
    case with_retry(Get, read_ecdsa_settled_tx) of
        not_found ->
            not_found;
        {ok, << Height:64, Hash:32/binary >>} ->
            {Height, Hash}
    end.

%% @doc Return the {TXID, Height, Hash} entries of the stored key-value pairs
%% with identifiers bigger than Cursor.
entries(Pairs, Cursor) ->
    [{TXID, Height, Hash}
        || {<< TXID:32/binary >>, << Height:64, Hash:32/binary >>} <- Pairs,
        TXID > Cursor].

%% @doc Return the key-value pairs storing Entries.
pairs(Entries) ->
    [{TXID, << Height:64, Hash/binary >>} || {TXID, Height, Hash} <- Entries].

read_state() ->
    Get = fun() -> ar_kv:get(?DB, ?STATE_KEY) end,
    case with_retry(Get, read_ecdsa_settled_state) of
        not_found ->
            not_found;
        {ok, Bin} ->
            binary_to_term(Bin)
    end.

write(Entries, State) ->
    write(Entries, [], State).

%% @doc Remove RemovedTXIDs and store Entries and State in one atomic batch.
write(Entries, RemovedTXIDs, State) ->
    KVs = [{?STATE_KEY, term_to_binary(State)} | pairs(Entries)],
    ok = with_retry(fun() -> ar_kv:write_batch(?DB, KVs, RemovedTXIDs) end,
            write_ecdsa_settled),
    record_checksum(State).

%% @doc Publish the block and the XOR of State as a single sample, so that a
%% scrape never pairs the block of one state with the XOR of another.
record_checksum({_Height, BH, _Count, XOR}) ->
    arweave_metrics:gauge_set(ecdsa_settled_checksum, checksum_sample(BH, XOR)),
    ok.

%% @doc Return the 48-bit integer made of the first 24 bits of BH followed by
%% the first 24 bits of XOR. The empty set below the first block gets 0.
checksum_sample(<<>>, XOR) ->
    XOR bsr 232;
checksum_sample(<< Prefix:24, _/binary >>, XOR) ->
    (Prefix bsl 24) bor (XOR bsr 232).

%% @doc Remove the state record and every transaction identifier.
reset() ->
    ok = with_retry(
        fun() -> ar_kv:delete_range(?DB, ?STATE_KEY, ?KEY_UPPER_BOUND) end,
        reset_ecdsa_settled).

settle_block(#block{ height = Height, indep_hash = BH,
        previous_block = PrevBH } = B) ->
    case read_state() of
        {SettledHeight, _SettledBH, _Count, _XOR}
                when Height =< SettledHeight ->
            ok;
        {SettledHeight, PrevBH, Count, XOR} when Height == SettledHeight + 1 ->
            Entries = lists:ukeysort(1, ecdsa_entries(B)),
            {Settled, New} = lists:partition(
                    fun({TXID, _, _}) -> is_settled(TXID, Height) end, Entries),
            log_already_settled([TXID || {TXID, _, _} <- Settled], Height),
            write(New,
                    {Height, BH, Count + length(New), xor_entries(New, XOR)});
        {SettledHeight, SettledBH, _, _} ->
            ?LOG_CRITICAL([{event, cannot_settle_ecdsa_block},
                    {settled_height, SettledHeight},
                    {settled_block, arweave_lib_util:encode(SettledBH)},
                    {height, Height},
                    {previous_block, arweave_lib_util:encode(PrevBH)}]),
            error({cannot_settle_ecdsa_block, SettledHeight, Height})
    end.

log_already_settled([], _Height) ->
    ok;
log_already_settled(TXIDs, Height) ->
    ?LOG_ERROR([{event, ecdsa_tx_already_settled}, {height, Height},
            {txs, [arweave_lib_util:encode(TXID) || TXID <- TXIDs]}]).

%% @doc Fold the SHA-256 hashes of the serialized entries into XOR.
xor_entries(Entries, XOR) ->
    lists:foldl(
        fun({TXID, Height, Hash}, Acc) ->
            << N:256 >> = crypto:hash(sha256,
                    << TXID/binary, Height:64, Hash/binary >>),
            Acc bxor N
        end,
        XOR,
        Entries
    ).

ecdsa_entries(#block{ txs = TXs, height = Height }) ->
    [{TX#tx.id, Height, signature_data_hash(TX)}
        || TX <- [read_tx(TXOrID) || TXOrID <- TXs], ar_tx:is_ecdsa_tx(TX)].

read_tx(#tx{} = TX) ->
    TX;
read_tx(TXID) ->
    with_retry(
        fun() ->
            Peers = ar_peers:get_trusted_peers(),
            case ar_http_iface_client:get_tx(Peers, TXID) of
                #tx{} = TX ->
                    TX;
                _ ->
                    {error, tx_not_found}
            end
        end,
        read_ecdsa_settled_block_tx).

read_block(BH, BlocksByHash) ->
    case maps:get(BH, BlocksByHash, not_found) of
        not_found ->
            with_retry(fun() -> read_block(BH) end, read_ecdsa_settled_block);
        B ->
            B
    end.

read_block(BH) ->
    case ar_storage:read_block(BH) of
        #block{} = B ->
            B;
        _ ->
            read_block_from_peers(BH)
    end.

read_block_from_peers(BH) ->
    Peers = ar_peers:get_trusted_peers(),
    case ar_http_iface_client:get_block_shadow(Peers, BH) of
        {_Peer, #block{ indep_hash = BH } = B, _Time, _Size} ->
            case ar_block:indep_hash(B) of
                BH ->
                    B;
                _ ->
                    {error, invalid_block}
            end;
        _ ->
            {error, block_not_found}
    end.

find_set(BI, Blocks, Target, Source) ->
    case read_state() of
        not_found ->
            bootstrap(BI, Target, Source, -1, none);
        {Height, BH, _Count, _XOR} ->
            case is_on_chain(Height, BH, BI) of
                true ->
                    case has_newer_set(Height, BI, Blocks, Target, Source) of
                        true ->
                            ar:console("Replacing the ECDSA settled set at "
                                    "the height ~B with a newer one.~n",
                                    [Height]),
                            MinHeight = peer_set_min_height(BI, Blocks, Target),
                            bootstrap(BI, Target, Source, MinHeight, Height);
                        false ->
                            ok
                    end;
                false ->
                    ar:console("The block ~s at the height ~B of the ECDSA "
                            "settled set is not in the block index.~n",
                            [arweave_lib_util:encode(BH), Height]),
                    bootstrap(BI, Target, Source, -1, none)
            end
    end.

%% @doc Return true if the block BH at Height is in BI or descends from its
%% tip.
is_on_chain(Height, BH, BI) ->
    is_in_block_index(Height, BH, BI) orelse descends_from_tip(Height, BH, BI).

%% @doc Return true if the stored block BH at Height, above the tip of BI,
%% descends from that tip according to the stored block index.
descends_from_tip(Height, BH, BI) ->
    TipHeight = length(BI) - 1,
    Height > TipHeight andalso
        case ar_storage:read_block_index() of
            not_found ->
                false;
            StoredBI ->
                {TipBH, _, _} = hd(BI),
                is_in_block_index(Height, BH, StoredBI)
                    andalso is_in_block_index(TipHeight, TipBH, StoredBI)
        end.

%% @doc Return true if Source has a set settled above Height, the height of the
%% stored set, at or below Target.
has_newer_set(Height, BI, Blocks, Target, peers) ->
    MinHeight = peer_set_min_height(BI, Blocks, Target),
    HasPeerSet = fun(Peer) -> has_peer_set(Peer, MinHeight, BI) end,
    %% The genesis file provides such a set if no peer does.
    Height < min(Target, genesis_file_height(BI))
        orelse (Height < MinHeight
                andalso lists:any(HasPeerSet, ar_peers:get_trusted_peers()));
has_newer_set(Height, BI, _Blocks, Target, {snapshot, Dir}) ->
    case with_snapshot_db(Dir, fun(Name) -> ar_kv:get(Name, ?STATE_KEY) end) of
        {ok, Bin} ->
            case catch binary_to_term(Bin, [safe]) of
                {SnapshotHeight, BH, _Count, _XOR} ->
                    min(SnapshotHeight, Target) > Height
                        andalso is_in_block_index(SnapshotHeight, BH, BI);
                _ ->
                    false
            end;
        _ ->
            false
    end;
has_newer_set(_Height, _BI, _Blocks, _Target, local) ->
    false.

%% @doc Return the lowest height of a peer set above which the blocks up to
%% Target are among Blocks, so settling them does not require the network.
peer_set_min_height(BI, Blocks, Target) ->
    min(Target, length(BI) - length(Blocks) - 1).

has_peer_set(Peer, MinHeight, BI) ->
    case ar_http_iface_client:get_ecdsa_settled_state(Peer) of
        {ok, {PeerHeight, BH, _Count, _XOR}} ->
            PeerHeight >= MinHeight
                andalso is_in_block_index(PeerHeight, BH, BI);
        _ ->
            false
    end.

%% @doc Replace the stored set with the one of Source. Take a peer set only if
%% it is settled at or above MinHeight and the genesis file set. Without such a
%% peer set, keep the stored set at StoredHeight unless the genesis file set is
%% newer.
bootstrap(_BI, -1, _Source, _MinHeight, _StoredHeight) ->
    reset(),
    write([], {-1, <<>>, 0, 0});
bootstrap(_BI, _Target, local, _MinHeight, _StoredHeight) ->
    {error, ecdsa_settled_set_not_found};
bootstrap(BI, _Target, {snapshot, Dir}, _MinHeight, _StoredHeight) ->
    import(Dir, BI);
bootstrap(BI, _Target, peers, MinHeight, StoredHeight) ->
    GenesisHeight = genesis_file_height(BI),
    MinHeight2 = max(MinHeight, GenesisHeight),
    case download(ar_peers:get_trusted_peers(), BI, MinHeight2) of
        ok ->
            ok;
        {error, _} = Error ->
            Error;
        not_found when is_integer(StoredHeight),
                StoredHeight >= GenesisHeight ->
            ok;
        not_found ->
            reset(),
            load_genesis_file(BI)
    end.

%% @doc Remove the transactions settled above Target, recomputing Count and
%% XOR from the remaining ones, in one atomic batch with the new state.
truncate(Target, BI) ->
    case read_state() of
        {Height, _BH, _Count, _XOR} when Height > Target ->
            ar:console("Truncating the ECDSA settled set from the height ~B "
                    "to ~B.~n", [Height, Target]),
            {Removed, Count, XOR} =
                    truncated_entries(<< 0:256 >>, Target, [], 0, 0),
            write([], Removed, {Target, block_hash(Target, BI), Count, XOR});
        _ ->
            ok
    end.

%% @doc Return {Removed, Count, XOR}, where Removed lists the identifiers
%% settled above Target and Count and XOR describe the remaining ones.
truncated_entries(Cursor, Target, Removed, Count, XOR) ->
    {ok, Entries} = with_retry(fun() -> get_chunk(Cursor) end,
            read_ecdsa_settled_chunk),
    {Kept, Above} = lists:partition(fun({_, H, _}) -> H =< Target end,
            Entries),
    Removed2 = [TXID || {TXID, _, _} <- Above] ++ Removed,
    Count2 = Count + length(Kept),
    XOR2 = xor_entries(Kept, XOR),
    case length(Entries) < ?ECDSA_SETTLED_CHUNK_SIZE of
        true ->
            {Removed2, Count2, XOR2};
        false ->
            {LastTXID, _, _} = lists:last(Entries),
            truncated_entries(LastTXID, Target, Removed2, Count2, XOR2)
    end.

settle_to(Target, BI, Blocks) ->
    {Height, _BH, _Count, _XOR} = read_state(),
    case Target > Height of
        true ->
            ar:console("Settling ECDSA transactions from the heights ~B "
                    "to ~B.~n", [Height + 1, Target]),
            TipHeight = length(BI) - 1,
            BlocksByHash = maps:from_list(
                    [{B#block.indep_hash, B} || B <- Blocks]),
            BHs = lists:reverse(lists:sublist(
                    lists:nthtail(TipHeight - Target, BI), Target - Height)),
            lists:foreach(
                fun({BH, _, _}) -> settle([read_block(BH, BlocksByHash)]) end,
                BHs
            );
        false ->
            ok
    end.

%% @doc Replace the stored set with the highest set of Peers settled at a block
%% of BI at or above MinHeight, or return not_found. Keep the stored set and
%% return {error, {ecdsa_settled_peer_set_mismatch, Peer, Peer2, Height}} if
%% the set of Peer disagrees with the set Peer2 reports at Height.
download(Peers, BI, MinHeight) ->
    Sets = lists:filtermap(
        fun(Peer) ->
            case ar_http_iface_client:get_ecdsa_settled_state(Peer) of
                {ok, State} ->
                    {true, {State, Peer}};
                Error ->
                    log_download_failure(Peer, Error),
                    false
            end
        end,
        Peers
    ),
    Sorted = lists:reverse(lists:sort(Sets)),
    download_sets(Sorted, Sorted, BI, MinHeight).

download_sets([], _PeerSets, _BI, _MinHeight) ->
    not_found;
download_sets([{{Height, BH, Count, _XOR} = State, Peer} = Set | Sets],
        PeerSets, BI, MinHeight) ->
    maybe
        true ?= is_in_block_index(Height, BH, BI)
                orelse {error, block_not_in_block_index},
        true ?= Height >= MinHeight orelse {error, set_too_old},
        LowerSets = lower_sets(Height, PeerSets -- [Set], BI),
        Download =
            fun(Name) -> download_set(Peer, Name, State, LowerSets, BI) end,
        ok ?= with_download_db(Download),
        ar:console("Downloaded ~B ECDSA settled transactions up to the "
                "height ~B from ~s. The sets of ~B other trusted peers "
                "match.~n",
                [Count, Height, arweave_lib_util:format_peer(Peer),
                length(LowerSets)]),
        ok
    else
        {error, {ecdsa_settled_peer_set_mismatch, _, _, _}} = Error ->
            Error;
        Error ->
            log_download_failure(Peer, Error),
            download_sets(Sets, PeerSets, BI, MinHeight)
    end.

%% @doc Return the sets settled at a block of BI at or below Height.
lower_sets(Height, Sets, BI) ->
    [Set || {{Height2, BH, _Count, _XOR}, _Peer} = Set <- Sets,
            Height2 =< Height, is_in_block_index(Height2, BH, BI)].

log_download_failure(Peer, Error) ->
    ?LOG_WARNING([{event, failed_to_download_ecdsa_settled},
            {peer, arweave_lib_util:format_peer(Peer)},
            {reason, io_lib:format("~p", [Error])}]).

%% @doc Download the set of Peer described by State into the database Name
%% and, if the set matches State and LowerSets, the sets the other peers report
%% at the same or lower blocks, replace the stored set with it.
download_set(Peer, Name, {Height, _BH, Count, XOR} = State, LowerSets, BI) ->
    maybe
        {ok, Read} ?= download_chunks(Peer, Name, << 0:256 >>, Height, {0, 0}),
        true ?= Read == {Count, XOR} orelse {error, ecdsa_settled_set_mismatch},
        ok ?= compare_sets(Name, Peer, LowerSets),
        %% The state record marks the set as verified, so that a start after
        %% a crash finishes the replacement (promote_download/1).
        ok ?= ar_kv:put(Name, ?STATE_KEY, term_to_binary(State)),
        replace(Name, BI)
    end.

%% @doc Return ok if, for each of the given sets, the entries of the database
%% Name settled at or below its height have its Count and XOR. Peer is where
%% the entries come from.
compare_sets(_Name, _Peer, []) ->
    ok;
compare_sets(Name, Peer, [{{Height, _BH, Count, XOR}, Peer2} | Sets]) ->
    maybe
        {ok, Settled, _Read} ?= copy_entries(Name, none, << 0:256 >>, Height,
                {0, 0}, {0, 0}),
        true ?= Settled == {Count, XOR}
                orelse {error,
                    {ecdsa_settled_peer_set_mismatch, Peer, Peer2, Height}},
        compare_sets(Name, Peer, Sets)
    end.

%% @doc Replace the stored set with the verified set of the database Name.
replace(Name, BI) ->
    reset(),
    Copy = fun() -> copy(Name, ?DB, BI, length(BI) - 1) end,
    {ok, _} = with_retry(Copy, copy_ecdsa_settled),
    ok.

%% @doc Replace the stored set with a newer set an interrupted download
%% verified for the chain of BI, and remove the download database unless
%% reading it failed.
promote_download(BI) ->
    Path = download_db_path(),
    case filelib:is_dir(Path) of
        false ->
            ok;
        true ->
            case do_promote_download(Path, BI) of
                ok ->
                    file:del_dir_r(Path);
                {error, Reason} ->
                    %% Keep the download for the next start, as the error may
                    %% be transient.
                    ?LOG_WARNING([
                            {event, failed_to_read_ecdsa_settled_download},
                            {reason, io_lib:format("~p", [Reason])}])
            end,
            ok
    end.

do_promote_download(Path, BI) ->
    maybe
        ok ?= ar_kv:open(#{ path => Path, name => ?DOWNLOAD_DB,
                log_path => Path }),
        try
            case verify(?DOWNLOAD_DB, BI) of
                {ok, Height} ->
                    case has_stored_set(Height, BI) of
                        true ->
                            ok;
                        false ->
                            ar:console("Resuming the replacement of the "
                                    "ECDSA settled set.~n"),
                            replace(?DOWNLOAD_DB, BI)
                    end;
                {error, Reason} when Reason == ecdsa_settled_set_not_found;
                        Reason == block_not_in_block_index;
                        Reason == ecdsa_settled_set_mismatch ->
                    %% The download was not verified or does not belong to
                    %% the chain.
                    ok;
                Error ->
                    Error
            end
        after
            ar_kv:close(?DOWNLOAD_DB)
        end
    end.

%% @doc Return true if the stored set belongs to the chain of BI and is settled
%% at or above Height.
has_stored_set(Height, BI) ->
    case read_state() of
        {StoredHeight, BH, _Count, _XOR} ->
            StoredHeight >= Height andalso is_on_chain(StoredHeight, BH, BI);
        not_found ->
            false
    end.

%% @doc Write the entries of Peer settled at or below Height to the database
%% Name and return {ok, Read} with their {Count, XOR}. The peer may settle more
%% blocks while we are downloading.
download_chunks(Peer, Name, Cursor, Height, Read) ->
    case ar_http_iface_client:get_ecdsa_settled_chunk(Peer, Cursor) of
        {ok, []} ->
            {ok, Read};
        {ok, [{FirstTXID, _, _} | _] = Entries} when FirstTXID > Cursor ->
            Settled = [Entry || {_, H, _} = Entry <- Entries, H =< Height],
            maybe
                ok ?= ar_kv:write_batch(Name, pairs(Settled)),
                Read2 = add_entries(Settled, Read),
                case length(Entries) < ?ECDSA_SETTLED_CHUNK_SIZE of
                    true ->
                        {ok, Read2};
                    false ->
                        {LastTXID, _, _} = lists:last(Entries),
                        download_chunks(Peer, Name, LastTXID, Height, Read2)
                end
            end;
        {ok, _Entries} ->
            {error, invalid_chunk};
        Error ->
            Error
    end.

%% @doc Call Fun with the name of a new empty database and remove the database
%% afterwards.
with_download_db(Fun) ->
    Name = ?DOWNLOAD_DB,
    Path = download_db_path(),
    maybe
        %% Start empty even if an earlier removal failed.
        ok ?= remove_dir(Path),
        %% Keep the RocksDB logs inside Path so they are removed with it.
        ok ?= ar_kv:open(#{ path => Path, name => Name, log_path => Path }),
        try
            Fun(Name)
        after
            ar_kv:close(Name),
            file:del_dir_r(Path)
        end
    end.

download_db_path() ->
    filename:join([arweave_config:get([data_dir]), ?ROCKS_DB_DIR,
            "ecdsa_settled_2_download_db"]).

remove_dir(Path) ->
    case file:del_dir_r(Path) of
        {error, enoent} ->
            ok;
        Reply ->
            Reply
    end.

%% @doc Replace the stored set with the one stored in the start_from_state
%% directory Dir if the latter matches its state record.
import(Dir, BI) ->
    Import =
        fun(Name) ->
            maybe
                {ok, _} ?= verify(Name, BI),
                reset(),
                copy(Name, ?DB, BI, length(BI) - 1)
            end
        end,
    case with_snapshot_db(Dir, Import) of
        {ok, Height} ->
            ar:console("Copied the ECDSA settled set up to the height ~B "
                    "from ~s.~n", [Height, Dir]),
            ok;
        {error, Reason} ->
            ar:console("Failed to copy the ECDSA settled set from ~s: ~p.~n",
                    [Dir, Reason]),
            {error, ecdsa_settled_set_not_found}
    end.

%% @doc Call Fun with the name of the opened set database of the
%% start_from_state directory Dir.
with_snapshot_db(Dir, Fun) ->
    Name = start_from_state_ecdsa_settled_2_db,
    Path = filename:join([Dir, ?ROCKS_DB_DIR, "ecdsa_settled_2_db"]),
    LogPath = filename:join([arweave_config:get([log_dir]), "rocksdb",
            "start_from_state"]),
    maybe
        %% Opening a missing database would create its directory.
        true ?= filelib:is_dir(Path) orelse {error, not_found},
        ok ?= ar_kv:open(#{ path => Path, name => Name, log_path => LogPath,
                readonly => true }),
        try
            Fun(Name)
        after
            ar_kv:close(Name)
        end
    end.

%% @doc Copy the set stored in SourceDB, whose block must be on the chain of
%% BI, to TargetDB, keeping the transactions settled up to the consensus window
%% below the block of BI at TipHeight. Return {ok, Height} with the height of
%% the copied set or {error, Reason}.
copy(SourceDB, TargetDB, BI, TipHeight) ->
    maybe
        {ok, {Height, _BH, Count, XOR}} ?= source_state(SourceDB, BI),
        Height2 = min(Height, target(TipHeight)),
        {ok, {Count2, XOR2}, Read} ?= copy_entries(SourceDB, TargetDB,
                << 0:256 >>, Height2, {0, 0}, {0, 0}),
        true ?= Read == {Count, XOR}
                orelse {error, ecdsa_settled_set_mismatch},
        State = {Height2, block_hash(Height2, BI), Count2, XOR2},
        ok ?= ar_kv:put(TargetDB, ?STATE_KEY, term_to_binary(State)),
        {ok, Height2}
    else
        not_found ->
            {error, ecdsa_settled_set_not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc Return {ok, Height} with the height of the set stored in SourceDB if
%% the set belongs to the chain of BI and matches its state record.
verify(SourceDB, BI) ->
    maybe
        {ok, {Height, _BH, Count, XOR}} ?= source_state(SourceDB, BI),
        {ok, _Copied, Read} ?= copy_entries(SourceDB, none, << 0:256 >>, -1,
                {0, 0}, {0, 0}),
        true ?= Read == {Count, XOR}
                orelse {error, ecdsa_settled_set_mismatch},
        {ok, Height}
    else
        not_found ->
            {error, ecdsa_settled_set_not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc Return {ok, State} with the state record of the set stored in SourceDB
%% if its block is on the chain of BI.
source_state(SourceDB, BI) ->
    maybe
        {ok, Bin} ?= ar_kv:get(SourceDB, ?STATE_KEY),
        {Height, BH, _Count, _XOR} = State = binary_to_term(Bin, [safe]),
        true ?= is_on_chain(Height, BH, BI)
                orelse {error, block_not_in_block_index},
        {ok, State}
    end.

%% @doc Copy the entries settled at or below Height to TargetDB, or only read
%% them if TargetDB is none. Return {ok, Copied, Read} with the {Count, XOR}
%% of the copied entries and of all the read ones.
copy_entries(SourceDB, TargetDB, Cursor, Height, Copied, Read) ->
    maybe
        {ok, Pairs} ?= ar_kv:get_next_n(SourceDB, Cursor, ?COPY_BATCH_SIZE),
        Entries = entries(Pairs, Cursor),
        Kept = [Entry || {_, H, _} = Entry <- Entries, H =< Height],
        ok ?= write_entries(TargetDB, Kept),
        Copied2 = add_entries(Kept, Copied),
        Read2 = add_entries(Entries, Read),
        case length(Pairs) < ?COPY_BATCH_SIZE of
            true ->
                {ok, Copied2, Read2};
            false ->
                {LastKey, _} = lists:last(Pairs),
                copy_entries(SourceDB, TargetDB, LastKey, Height, Copied2,
                        Read2)
        end
    end.

write_entries(none, _Entries) ->
    ok;
write_entries(DB, Entries) ->
    ar_kv:write_batch(DB, pairs(Entries)).

add_entries(Entries, {Count, XOR}) ->
    {Count + length(Entries), xor_entries(Entries, XOR)}.

%% A chain the genesis file does not describe starts from the empty set below
%% its first block.
load_genesis_file(BI) ->
    BH = arweave_lib_util:decode(?GENESIS_BH),
    case is_in_block_index(?GENESIS_HEIGHT, BH, BI) of
        true ->
            {ok, Bin} = file:read_file(?GENESIS_FILE),
            Lines = binary:split(Bin, << "\n" >>, [global, trim_all]),
            load_genesis_entries(?GENESIS_HEIGHT, BH, Lines);
        false ->
            write([], {-1, <<>>, 0, 0})
    end.

%% @doc Return the height of the set of the genesis file if its block is in BI,
%% or -1.
genesis_file_height(BI) ->
    BH = arweave_lib_util:decode(?GENESIS_BH),
    case is_in_block_index(?GENESIS_HEIGHT, BH, BI) of
        true ->
            ?GENESIS_HEIGHT;
        false ->
            -1
    end.

is_in_block_index(Height, BH, BI) ->
    Height < length(BI) andalso block_hash(Height, BI) == BH.

%% @doc Return the height of the block at the consensus window below
%% TipHeight, or -1.
target(TipHeight) ->
    Window = arweave_lib_constants:get_consensus_window_size(),
    max(-1, TipHeight - Window).

%% @doc Return the hash of the block at Height in BI. The height -1 gets the
%% previous block hash of the genesis block.
block_hash(-1, _BI) ->
    <<>>;
block_hash(Height, BI) ->
    element(1, lists:nth(length(BI) - Height, BI)).

load_genesis_entries(Height, BH, Lines) ->
    Entries = [parse_genesis_line(Line) || Line <- Lines],
    write(Entries, {Height, BH, length(Entries), xor_entries(Entries, 0)}),
    ar:console("Loaded ~B ECDSA settled transactions up to the height ~B "
            "from the genesis file.~n", [length(Entries), Height]).

%% @doc Return the {TXID, Height, Hash} entry of a "TXID Height Hash" line of
%% the genesis file.
parse_genesis_line(Line) ->
    [EncodedTXID, EncodedHeight, EncodedHash] =
            binary:split(Line, << " " >>, [global]),
    << _:32/binary >> = TXID = arweave_lib_util:decode(EncodedTXID),
    << _:32/binary >> = Hash = arweave_lib_util:decode(EncodedHash),
    {TXID, binary_to_integer(EncodedHeight), Hash}.

with_retry(Fun, Event) ->
    with_retry(Fun, Event, 1).

with_retry(Fun, Event, Attempt) ->
    case Fun() of
        {error, Reason} ->
            log_failure(Event, Reason, Attempt),
            timer:sleep(?RETRY_INTERVAL_MS),
            with_retry(Fun, Event, Attempt + 1);
        Reply ->
            Reply
    end.

log_failure(Event, Reason, Attempt) when Attempt < ?FAILURES_BEFORE_CRITICAL ->
    ?LOG_WARNING([{event, Event}, {attempt, Attempt},
            {reason, io_lib:format("~p", [Reason])}]);
log_failure(Event, Reason, Attempt) ->
    ?LOG_CRITICAL([{event, Event}, {attempt, Attempt},
            {reason, io_lib:format("~p", [Reason])}]).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

genesis_file_test() ->
    {ok, Bin} = file:read_file(?GENESIS_FILE),
    Lines = binary:split(Bin, << "\n" >>, [global, trim_all]),
    Entries = [parse_genesis_line(Line) || Line <- Lines],
    TXIDs = [TXID || {TXID, _, _} <- Entries],
    ?assertEqual(lists:usort(TXIDs), TXIDs),
    ?assertEqual([], [Entry || {_, Height, _} = Entry <- Entries,
            Height > ?GENESIS_HEIGHT]).

download_test_() ->
    ar_test_util:with_mocked([
        {ar_http_iface_client, get_ecdsa_settled_state, fun peer_state/1},
        {ar_http_iface_client, get_ecdsa_settled_chunk,
            fun(_Peer, _Cursor) -> {ok, []} end},
        {ar_kv, delete_range, fun(?DB, _Start, _End) -> ok end},
        {ar_kv, write_batch,
            fun(?DB, _KVs) -> ok;
               (Name, KVs) -> meck:passthrough([Name, KVs])
            end},
        {ar_kv, put,
            fun(?DB, _Key, _Value) -> ok;
               (Name, Key, Value) -> meck:passthrough([Name, Key, Value])
            end}],
        fun test_download/0).

test_download() ->
    BI = genesis_block_index(),
    Downloaded = fun(Peer) ->
        meck:called(ar_http_iface_client, get_ecdsa_settled_chunk, [Peer, '_'])
    end,
    %% Skip the sets older than the genesis file set.
    ?assertEqual(ok, download([{1, 1, 1, 1, 1}, {2, 2, 2, 2, 2},
            {3, 3, 3, 3, 3}], BI, ?GENESIS_HEIGHT)),
    ?assertNot(Downloaded({1, 1, 1, 1, 1})),
    ?assertNot(Downloaded({2, 2, 2, 2, 2})),
    ?assert(Downloaded({3, 3, 3, 3, 3})),
    ?assertEqual(not_found, download([{1, 1, 1, 1, 1}], BI, ?GENESIS_HEIGHT)),
    %% Skip a set above the tip.
    meck:reset(ar_http_iface_client),
    ?assertEqual(ok, download([{9, 9, 9, 9, 9}, {3, 3, 3, 3, 3}], BI,
            ?GENESIS_HEIGHT)),
    ?assertNot(Downloaded({9, 9, 9, 9, 9})),
    ?assert(Downloaded({3, 3, 3, 3, 3})),
    %% Skip the sets below the required height.
    meck:reset(ar_http_iface_client),
    ?assertEqual(ok, download([{4, 4, 4, 4, 4}, {3, 3, 3, 3, 3}], BI,
            ?GENESIS_HEIGHT + 1)),
    ?assertNot(Downloaded({4, 4, 4, 4, 4})),
    ?assert(Downloaded({3, 3, 3, 3, 3})),
    %% Take the highest set.
    meck:reset(ar_http_iface_client),
    ?assertEqual(ok, download([{4, 4, 4, 4, 4}, {3, 3, 3, 3, 3}], BI,
            ?GENESIS_HEIGHT)),
    ?assertNot(Downloaded({4, 4, 4, 4, 4})),
    ?assert(Downloaded({3, 3, 3, 3, 3})),
    %% A chain without the block of the genesis file accepts the empty set.
    ?assertEqual(ok, download([{1, 1, 1, 1, 1}], [{<<"tip">>, 0, <<>>}], -1)),
    ?assert(meck:called(ar_kv, put,
            [?DB, ?STATE_KEY, term_to_binary({-1, <<>>, 0, 0})])),
    %% A download replaces the database a crash left and removes its own.
    DownloadDB = ?DOWNLOAD_DB,
    Path = download_db_path(),
    ok = ar_kv:open(#{ path => Path, name => DownloadDB, log_path => Path }),
    ok = ar_kv:put(DownloadDB, << 1:256 >>, << 1:64, 0:256 >>),
    ar_kv:close(DownloadDB),
    ?assertEqual(ok, download([{3, 3, 3, 3, 3}], BI, ?GENESIS_HEIGHT)),
    ?assertNot(filelib:is_dir(Path)),
    %% A set not matching its state leaves the stored set untouched.
    meck:reset(ar_kv),
    meck:expect(ar_http_iface_client, get_ecdsa_settled_chunk,
            fun(_Peer, _Cursor) -> {ok, [{<< 1:256 >>, 1, << 0:256 >>}]} end),
    ?assertEqual(not_found, download([{3, 3, 3, 3, 3}], BI, ?GENESIS_HEIGHT)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    ?assertNot(meck:called(ar_kv, write_batch, [?DB, '_'])),
    ?assertNot(meck:called(ar_kv, put, [?DB, '_', '_'])).

compare_peer_sets_test_() ->
    ar_test_util:with_mocked([
        {ar_http_iface_client, get_ecdsa_settled_state, fun peer_state/1},
        {ar_http_iface_client, get_ecdsa_settled_chunk,
            fun(_Peer, _Cursor) -> {ok, []} end},
        {ar_kv, delete_range, fun(?DB, _Start, _End) -> ok end},
        {ar_kv, write_batch,
            fun(?DB, _KVs) -> ok;
               (Name, KVs) -> meck:passthrough([Name, KVs])
            end},
        {ar_kv, put,
            fun(?DB, _Key, _Value) -> ok;
               (Name, Key, Value) -> meck:passthrough([Name, Key, Value])
            end}],
        fun test_compare_peer_sets/0).

test_compare_peer_sets() ->
    BI = genesis_block_index(),
    TipPeer = {3, 3, 3, 3, 3},
    LowerPeer = {4, 4, 4, 4, 4},
    Peers = [LowerPeer, TipPeer],
    GenesisBH = arweave_lib_util:decode(?GENESIS_BH),
    Lower = test_entry(1, ?GENESIS_HEIGHT),
    Tip = test_entry(2, ?GENESIS_HEIGHT + 1),
    TipState = {?GENESIS_HEIGHT + 1, <<"tip">>, 2,
            xor_entries([Lower, Tip], 0)},
    SetLowerState = fun(LowerState) ->
        meck:reset(ar_kv),
        meck:expect(ar_http_iface_client, get_ecdsa_settled_state,
            fun(Peer) when Peer == TipPeer -> {ok, TipState};
               (Peer) when Peer == LowerPeer -> {ok, LowerState}
            end)
    end,
    %% The two entries fill a chunk of the test build, so the download asks
    %% for the next one.
    meck:expect(ar_http_iface_client, get_ecdsa_settled_chunk,
        fun(_Peer, << 0:256 >>) -> {ok, [Lower, Tip]};
           (_Peer, _Cursor) -> {ok, []}
        end),
    %% The lower set holds the entries of the tip set settled up to its block.
    SetLowerState({?GENESIS_HEIGHT, GenesisBH, 1, xor_entries([Lower], 0)}),
    ?assertEqual(ok, download(Peers, BI, ?GENESIS_HEIGHT)),
    ?assert(meck:called(ar_kv, delete_range, '_')),
    %% The same holds for a set at the block of the tip set.
    SetLowerState(TipState),
    ?assertEqual(ok, download(Peers, BI, ?GENESIS_HEIGHT)),
    ?assert(meck:called(ar_kv, delete_range, '_')),
    %% A set of another chain is not compared.
    SetLowerState({?GENESIS_HEIGHT, <<"other">>, 0, 0}),
    ?assertEqual(ok, download(Peers, BI, ?GENESIS_HEIGHT)),
    ?assert(meck:called(ar_kv, delete_range, '_')),
    %% A lower set with another entry, without the entry or with another
    %% number of entries leaves the stored set untouched.
    Mismatch = {error, {ecdsa_settled_peer_set_mismatch, TipPeer, LowerPeer,
            ?GENESIS_HEIGHT}},
    lists:foreach(
        fun({Count, XOR}) ->
            SetLowerState({?GENESIS_HEIGHT, GenesisBH, Count, XOR}),
            ?assertEqual(Mismatch, download(Peers, BI, ?GENESIS_HEIGHT)),
            ?assertNot(meck:called(ar_kv, delete_range, '_')),
            ?assertNot(meck:called(ar_kv, write_batch, [?DB, '_'])),
            ?assertNot(meck:called(ar_kv, put, [?DB, '_', '_'])),
            ?assertNot(filelib:is_dir(download_db_path()))
        end,
        [{1, xor_entries([Tip], 0)}, {0, 0}, {2, xor_entries([Lower], 0)}]
    ).

checksum_sample_test() ->
    %% The first 24 bits of the block hash, then the first 24 bits of the XOR.
    BH = << 16#abcdef:24, 0:360 >>,
    XOR = (16#123456 bsl 232) bor 16#ffffff,
    ?assertEqual(16#abcdef123456, checksum_sample(BH, XOR)),
    ?assertEqual(16#abcdef000000, checksum_sample(BH, 0)),
    ?assertEqual(0, checksum_sample(<<>>, 0)),
    ?assertEqual(ok, record_checksum({1, BH, 1, XOR})),
    ?assertEqual(16#abcdef123456,
            arweave_metrics:gauge_value(ecdsa_settled_checksum)).

has_newer_set_test_() ->
    SnapshotDB = start_from_state_ecdsa_settled_2_db,
    ar_test_util:with_mocked([
        {ar_kv, open,
            fun(#{ name := Name }) when Name == SnapshotDB -> ok;
               (Args) -> meck:passthrough([Args])
            end},
        {ar_kv, close,
            fun(Name) when Name == SnapshotDB -> ok;
               (Name) -> meck:passthrough([Name])
            end},
        {ar_peers, get_trusted_peers, fun() -> [] end},
        {ar_http_iface_client, get_ecdsa_settled_state, fun peer_state/1}],
        fun test_has_newer_set/0).

test_has_newer_set() ->
    Dir = test_snapshot_dir(),
    ok = filelib:ensure_dir(
            filename:join([Dir, ?ROCKS_DB_DIR, "ecdsa_settled_2_db"]) ++ "/"),
    try
        test_has_newer_set(Dir)
    after
        file:del_dir_r(Dir)
    end.

test_has_newer_set(Dir) ->
    BI = genesis_block_index(),
    Target = ?GENESIS_HEIGHT + 1,
    Snapshot = {snapshot, Dir},
    SetSnapshotState = fun(Height, BH) ->
        meck:expect(ar_kv, get,
            fun(start_from_state_ecdsa_settled_2_db, ?STATE_KEY) ->
                    {ok, term_to_binary({Height, BH, 0, 0})};
               (DB, Key) ->
                    meck:passthrough([DB, Key])
            end)
    end,
    SetPeers = fun(Peers) ->
        meck:expect(ar_peers, get_trusted_peers, fun() -> Peers end)
    end,
    %% A join replaces a set older than the genesis file set.
    ?assert(has_newer_set(-1, BI, [], Target, peers)),
    ?assert(has_newer_set(?GENESIS_HEIGHT - 1, BI, [], Target, peers)),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [], Target, peers)),
    ?assertNot(has_newer_set(-1, [{<<"tip">>, 0, <<>>}], [], 0, peers)),
    %% A join replaces a set below the given blocks with a peer set from which
    %% the given blocks settle the rest up to Target.
    SetPeers([{4, 4, 4, 4, 4}, {3, 3, 3, 3, 3}]),
    ?assert(has_newer_set(?GENESIS_HEIGHT, BI, [], Target, peers)),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [#block{}], Target, peers)),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [], ?GENESIS_HEIGHT, peers)),
    SetPeers([{4, 4, 4, 4, 4}]),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [], Target, peers)),
    %% The same holds on a chain without the block of the genesis file.
    ShortBI = [{<<"b4">>, 0, <<>>}, {<<"b3">>, 0, <<>>}, {<<"b2">>, 0, <<>>},
            {<<"b1">>, 0, <<>>}, {<<"b0">>, 0, <<>>}],
    TwoBlocks = [#block{}, #block{}],
    SetPeers([{6, 6, 6, 6, 6}]),
    ?assert(has_newer_set(-1, ShortBI, [], 4, peers)),
    %% A lagging peer set still requires the network.
    SetPeers([{5, 5, 5, 5, 5}]),
    ?assertNot(has_newer_set(-1, ShortBI, [], 4, peers)),
    %% A peer set above the tip is not on the chain we know.
    SetPeers([{7, 7, 7, 7, 7}]),
    ?assertNot(has_newer_set(-1, ShortBI, [], 4, peers)),
    %% With the blocks 3 and 4 given, a peer set at 2 suffices.
    SetPeers([{8, 8, 8, 8, 8}]),
    ?assert(has_newer_set(-1, ShortBI, TwoBlocks, 4, peers)),
    ?assertNot(has_newer_set(-1, ShortBI, [], 4, peers)),
    ?assertNot(has_newer_set(2, ShortBI, TwoBlocks, 4, peers)),
    %% A start from a snapshot replaces a set older than the snapshot set,
    %% counting only the part at or below Target.
    SetSnapshotState(?GENESIS_HEIGHT + 1, <<"tip">>),
    ?assert(has_newer_set(?GENESIS_HEIGHT, BI, [], Target, Snapshot)),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT + 1, BI, [], Target, Snapshot)),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [], ?GENESIS_HEIGHT,
            Snapshot)),
    %% The snapshot set must be on the chain.
    SetSnapshotState(?GENESIS_HEIGHT + 1, <<"other">>),
    ?assertNot(has_newer_set(?GENESIS_HEIGHT, BI, [], Target, Snapshot)),
    meck:expect(ar_kv, open, fun(_Args) -> {error, not_found} end),
    ?assertNot(has_newer_set(-1, BI, [], Target, Snapshot)),
    ?assertNot(has_newer_set(-1, BI, [], Target, local)).

import_test_() ->
    ar_test_util:with_mocked([
        {ar_kv, delete_range, fun(?DB, _Start, _End) -> ok end},
        {ar_kv, write_batch,
            fun(?DB, _KVs) -> ok;
               (Name, KVs) -> meck:passthrough([Name, KVs])
            end},
        {ar_kv, put,
            fun(?DB, _Key, _Value) -> ok;
               (Name, Key, Value) -> meck:passthrough([Name, Key, Value])
            end}],
        fun test_import/0).

test_import() ->
    Dir = test_snapshot_dir(),
    Path = filename:join([Dir, ?ROCKS_DB_DIR, "ecdsa_settled_2_db"]),
    file:del_dir_r(Dir),
    try
        test_import(Dir, Path)
    after
        file:del_dir_r(Dir)
    end.

test_import(Dir, Path) ->
    BI = genesis_block_index(),
    NotFound = {error, ecdsa_settled_set_not_found},
    %% A missing set is not created.
    ?assertEqual(NotFound, import(Dir, BI)),
    ?assertNot(filelib:is_dir(Path)),
    %% A set not matching its state leaves the stored set untouched.
    Entry = test_entry(1, 1),
    write_set(Path, [Entry], {?GENESIS_HEIGHT + 1, <<"tip">>, 2, 0}),
    ?assertEqual(NotFound, import(Dir, BI)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    ?assertNot(meck:called(ar_kv, write_batch, [?DB, '_'])),
    ?assertNot(meck:called(ar_kv, put, [?DB, '_', '_'])),
    %% A matching set replaces the stored set.
    write_set(Path, [],
            {?GENESIS_HEIGHT + 1, <<"tip">>, 1, xor_entries([Entry], 0)}),
    ?assertEqual(ok, import(Dir, BI)),
    ?assert(meck:called(ar_kv, delete_range, '_')),
    ?assert(meck:called(ar_kv, write_batch, [?DB, pairs([Entry])])).

promote_download_test_() ->
    ar_test_util:with_mocked([
        {ar_kv, get,
            fun(?DB, ?STATE_KEY) -> not_found;
               (Name, Key) -> meck:passthrough([Name, Key])
            end},
        {ar_storage, read_block_index, fun() -> not_found end},
        {ar_kv, delete_range, fun(?DB, _Start, _End) -> ok end},
        {ar_kv, write_batch,
            fun(?DB, _KVs) -> ok;
               (Name, KVs) -> meck:passthrough([Name, KVs])
            end},
        {ar_kv, put,
            fun(?DB, _Key, _Value) -> ok;
               (Name, Key, Value) -> meck:passthrough([Name, Key, Value])
            end}],
        fun test_promote_download/0).

test_promote_download() ->
    BI = genesis_block_index(),
    Path = download_db_path(),
    Entry = test_entry(1, 1),
    TipHeight = ?GENESIS_HEIGHT + 1,
    TipState = {TipHeight, <<"tip">>, 1, xor_entries([Entry], 0)},
    %% A download stopped before its verification is removed.
    write_set(Path, [Entry], none),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    ?assertNot(meck:called(ar_kv, write_batch, [?DB, '_'])),
    %% A verified download replaces the stored set, as when the node stopped
    %% while copying it.
    write_set(Path, [Entry], TipState),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    ?assert(meck:called(ar_kv, delete_range, '_')),
    ?assert(meck:called(ar_kv, write_batch, [?DB, pairs([Entry])])),
    ?assert(meck:called(ar_kv, put, [?DB, ?STATE_KEY, '_'])),
    %% A verified download is kept if opening or reading it fails, and
    %% replaces the stored set at the next start.
    meck:reset(ar_kv),
    write_set(Path, [Entry], TipState),
    meck:expect(ar_kv, open,
        fun(#{ name := ?DOWNLOAD_DB }) -> {error, failed};
           (Args) -> meck:passthrough([Args])
        end),
    ?assertEqual(ok, promote_download(BI)),
    ?assert(filelib:is_dir(Path)),
    meck:expect(ar_kv, open, fun(Args) -> meck:passthrough([Args]) end),
    meck:expect(ar_kv, get,
        fun(?DOWNLOAD_DB, ?STATE_KEY) -> {error, eio};
           (?DB, ?STATE_KEY) -> not_found;
           (Name, Key) -> meck:passthrough([Name, Key])
        end),
    ?assertEqual(ok, promote_download(BI)),
    ?assert(filelib:is_dir(Path)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    expect_stored_state(not_found),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    ?assert(meck:called(ar_kv, write_batch, [?DB, pairs([Entry])])),
    %% A verified download is removed if the stored set is as new.
    meck:reset(ar_kv),
    write_set(Path, [Entry], TipState),
    expect_stored_state(TipState),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    expect_stored_state(not_found),
    %% A verified download above the tip of BI is removed if the stored block
    %% index is of another chain.
    Entry2 = test_entry(2, TipHeight + 1),
    AboveState = {TipHeight + 1, <<"above">>, 2,
            xor_entries([Entry, Entry2], 0)},
    Above = {<<"above">>, 0, <<>>},
    write_set(Path, [Entry, Entry2], AboveState),
    meck:expect(ar_storage, read_block_index,
            fun() -> [Above, {<<"other">>, 0, <<>>} | tl(BI)] end),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    ?assertNot(meck:called(ar_kv, delete_range, '_')),
    %% It is copied up to the target of BI if the stored block index has both
    %% blocks.
    write_set(Path, [Entry, Entry2], AboveState),
    meck:expect(ar_storage, read_block_index, fun() -> [Above | BI] end),
    ?assertEqual(ok, promote_download(BI)),
    ?assertNot(filelib:is_dir(Path)),
    Target = target(TipHeight),
    State = {Target, block_hash(Target, BI), 1, xor_entries([Entry], 0)},
    ?assert(meck:called(ar_kv, write_batch, [?DB, pairs([Entry])])),
    ?assert(meck:called(ar_kv, put, [?DB, ?STATE_KEY, term_to_binary(State)])).

%% @doc Make the stored set have the state record State, or none if State is
%% not_found.
expect_stored_state(State) ->
    meck:expect(ar_kv, get,
        fun(?DB, ?STATE_KEY) when State == not_found ->
                not_found;
           (?DB, ?STATE_KEY) ->
                {ok, term_to_binary(State)};
           (Name, Key) ->
                meck:passthrough([Name, Key])
        end).

test_snapshot_dir() ->
    filename:join(arweave_config:get([data_dir]), "ecdsa_settled_snapshot").

%% @doc Add the entries and the state record, unless State is none, to the set
%% database at Path.
write_set(Path, Entries, State) ->
    Name = ecdsa_settled_test_db,
    %% Keep the RocksDB logs inside Path so they are removed with it.
    ok = ar_kv:open(#{ path => Path, name => Name, log_path => Path }),
    ok = ar_kv:write_batch(Name, pairs(Entries)),
    case State of
        none ->
            ok;
        _ ->
            ok = ar_kv:put(Name, ?STATE_KEY, term_to_binary(State))
    end,
    ar_kv:close(Name).

%% @doc Return an entry with the identifier N settled at Height.
test_entry(N, Height) ->
    {<< N:256 >>, Height, crypto:hash(sha256, << N:256 >>)}.

%% @doc Return a block index from the genesis file block - 1 to the block
%% above it.
genesis_block_index() ->
    Entry = fun(BH) -> {BH, 0, <<>>} end,
    GenesisBH = arweave_lib_util:decode(?GENESIS_BH),
    [Entry(<<"tip">>), Entry(GenesisBH), Entry(<<"old">>)
            | lists:duplicate(?GENESIS_HEIGHT - 1, Entry(<<"filler">>))].

%% @doc Return the set state of a test peer.
peer_state({1, 1, 1, 1, 1}) ->
    {ok, {-1, <<>>, 0, 0}};
peer_state({2, 2, 2, 2, 2}) ->
    {ok, {?GENESIS_HEIGHT - 1, <<"old">>, 0, 0}};
peer_state({3, 3, 3, 3, 3}) ->
    {ok, {?GENESIS_HEIGHT + 1, <<"tip">>, 0, 0}};
peer_state({4, 4, 4, 4, 4}) ->
    {ok, {?GENESIS_HEIGHT, arweave_lib_util:decode(?GENESIS_BH), 0, 0}};
peer_state({5, 5, 5, 5, 5}) ->
    {ok, {1, <<"b1">>, 0, 0}};
peer_state({6, 6, 6, 6, 6}) ->
    {ok, {4, <<"b4">>, 0, 0}};
peer_state({7, 7, 7, 7, 7}) ->
    {ok, {5, <<"b5">>, 0, 0}};
peer_state({8, 8, 8, 8, 8}) ->
    {ok, {2, <<"b2">>, 0, 0}};
peer_state({9, 9, 9, 9, 9}) ->
    {ok, {?GENESIS_HEIGHT + 2, <<"future">>, 0, 0}}.

-endif.
