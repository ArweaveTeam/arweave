%%% @doc Per-StoreID data-root and tx-index owner. Wraps the data_root_index
%%% / data_root_offset_index / tx_index / tx_offset_index column families.
%%%
%%% Process model: one singleton gen_server (`ar_data_roots') registered
%%% globally. The gen_server serializes the multi-step writes performed by
%%% `store_block/5' so that two concurrent block confirmations can't race
%%% on the data_root_index keys for the same offset. Read-only library
%%% functions remain callable from any process via direct `ar_kv' lookups.
-module(ar_data_roots).
-test_category([fast]).

-behaviour(gen_server).

-export([
         start_link/0,
         open_index_db/3,
         open_index_db/4,
         column_family/1,
         keys_column_family/1,
         legacy_db/1,
         keys_db/1,
         add_block_data_roots/3,
         store_block/5,
         store_block_async/5,
         store_block_sync/5, store_block_sync/6,
         get_entry/2,
         remove_range/3,
         iterator/3,
         next/1,
         reset/1,
         id/1,
         id/2,
         index_key/3,
         get_block/1,
         verify_stored_block/2,
         verify_stored_blocks/4,
         get_removed_block/2,
         validate_data_roots/4,
         validate_data_roots/5,
         are_synced/2,
         are_synced/4,
         is_synced/2,
         repair/3,
         %% Block lifecycle helpers.
         add_block/3,
         update_tx_index/3,
         repair_data_root_offset_index/2,
         remove_tx_index_range/3
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([data_root_entry/0, data_root_entries/0]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The prefix of the migrations index keys under which remove_block/3 keeps the
%% removed entries of a block.
-define(REMOVED_BLOCK_KEY_PREFIX, <<"data_root_verification_removed_block">>).
%% Each store finishes its pending removal before starting another batch.
-define(PENDING_REMOVAL_KEY, <<"data_root_verification_pending_removal">>).

-type data_root_entry() :: {DataRoot :: binary(), TXSize :: non_neg_integer(),
                            TXStartOffset :: non_neg_integer(), TXPath :: binary()}.
-type data_root_entries() :: [data_root_entry()].
%%%===================================================================
%%% Public: gen_server lifecycle
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Asynchronously store a block's data roots into StoreID's index.
%% Serialized through the ar_data_roots gen_server so concurrent block
%% confirmations can't race on the same offset.
store_block_async(BlockStart, BlockEnd, TXRoot, DataRootEntries, StoreID) ->
    BlockSize = BlockEnd - BlockStart,
    gen_server:cast(?MODULE,
                    {store_block, BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID}).

%% @doc Synchronous variant of store_block_async/5. Blocks until the write
%% completes. Used by callers that need to observe the new state immediately
%% (e.g. test harnesses).
store_block_sync(BlockStart, BlockEnd, TXRoot, DataRootEntries, StoreID) ->
    store_block_sync(BlockStart, BlockEnd, TXRoot, DataRootEntries, StoreID, 120000).

%% @doc store_block_sync/5 with an explicit gen_server call timeout. Callers with
%% no client to bound the wait (e.g. one-shot bulk seeding) can pass `infinity'.
store_block_sync(BlockStart, BlockEnd, TXRoot, DataRootEntries, StoreID, Timeout) ->
    BlockSize = BlockEnd - BlockStart,
    gen_server:call(?MODULE,
                    {store_block, BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID}, Timeout).

%%%===================================================================
%%% Public: DB configuration
%%%===================================================================
open_index_db(Dir, StoreID, BloomFilterOpts) ->
    open_index_db(Dir, "ar_data_sync_data_root_index_db", StoreID, BloomFilterOpts).

open_index_db(Dir, DBName, StoreID, BloomFilterOpts) ->
    ar_kv:open(#{
                 path => filename:join(Dir, DBName),
                 name => index_db(StoreID),
                 options => ar_kv:db_options(100) ++ BloomFilterOpts
                }).

column_family(Opts) ->
    {"data_root_index", Opts}.

keys_column_family(Opts) ->
    {"data_root_offset_index", Opts}.

legacy_db(StoreID) ->
    {data_root_index_old, StoreID}.

%% A reference to the on-disk key-value storage mapping
%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootIDs}.
%% Each key in DataRootIDs is a << DataRoot/binary, TXSize:256 >> binary.
%% Used to remove orphaned entries from DataRootIndex.
keys_db(StoreID) ->
    {data_root_offset_index, StoreID}.

%%%===================================================================
%%% Public: Block data root entries
%%%===================================================================
add_block_data_roots([], _BlockStart, _StoreID) ->
    {ok, sets:new()};
add_block_data_roots(SizeTaggedTXs, BlockStart, StoreID) ->
    {TXRoot, BlockSize, DataRootEntries} =
        build_block_data_root_entries(BlockStart, SizeTaggedTXs),
    case BlockSize > 0 of
        true ->
            store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID);
        false ->
            {ok, sets:new()}
    end.

%% @doc Build `{TXRoot, BlockSize, DataRootEntries}` for the given size-tagged txs.
build_block_data_root_entries(BlockStart, SizeTaggedTXs) ->
    SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
    {TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
    {BlockSize, DataRootEntries} = lists:foldl(
                                     fun ({_, Offset}, {Offset, _} = Acc) ->
                                             Acc;
                                         ({{padding, _}, Offset}, {_, DataRootEntriesAcc}) ->
                                             {Offset, DataRootEntriesAcc};
                                         ({{_, DataRoot}, Offset}, {_, DataRootEntriesAcc}) when byte_size(DataRoot) < 32 ->
                                             {Offset, DataRootEntriesAcc};
                                         ({{_, DataRoot}, TXEndOffset}, {PrevOffset, DataRootEntriesAcc}) ->
                                             TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
                                             TXOffset = BlockStart + PrevOffset,
                                             TXSize = TXEndOffset - PrevOffset,
                                             {TXEndOffset,
                                              [{DataRoot, TXSize, TXOffset, TXPath} | DataRootEntriesAcc]}
                                     end,
                                     {0, []},
                                     SizeTaggedTXs
                                    ),
    {TXRoot, BlockSize, DataRootEntries}.

%% @doc Store all data roots for a given block.
%% DataRootEntries is a list of `{DataRoot, TXSize, TXStartOffset, TXPath}` tuples.
store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID) ->
                                                % Update index_db
    lists:foreach(
      fun({DataRoot, TXSize, TXStartOffset, TXPath}) ->
              ok = ar_kv:put(index_db(StoreID),
                             index_key(DataRoot, TXSize, TXStartOffset), TXPath)
      end,
      DataRootEntries
     ),
                                                % Update keys_db
    DataRootIDs = sets:from_list([
                                  id(DataRoot, TXSize)
                                  || {DataRoot, TXSize, _TXStart, _TXPath} <- DataRootEntries
                                 ]),
    ok = ar_kv:put(keys_db(StoreID),
                   << BlockStart:?OFFSET_KEY_BITSIZE >>,
                   term_to_binary({TXRoot, BlockSize, DataRootIDs})),
    {ok, DataRootIDs}.

%% @doc Get the data roots for the block containing the given block offset.
%% Return only entries corresponding to non-empty transactions.
%% Return the complete list of entries in the order they appear in the data root index,
%% which corresponds to sorted #tx records in the block.
%% Return {ok, {TXRoot, BlockSize, DataRootEntries}}
%% or {error, Reason}.
get_block(Offset) ->
    case Offset >= ar_disk_pool:get_threshold() of
        true ->
            {error, not_found};
        false ->
            case ar_block_index:get_block_bounds(Offset) of
                not_found ->
                    {error, not_found};
                Bounds ->
                    do_get_block(Offset, Bounds)
            end
    end.

do_get_block(Offset, {BlockStart, BlockEnd, TXRoot}) ->
    true = Offset >= BlockStart andalso Offset < BlockEnd,
    case get_keys(BlockStart, ?DEFAULT_MODULE) of
        not_found ->
            {error, not_found};
        {ok, Bin} ->
            {TXRoot2, BlockSize, DataRootIDs} = binary_to_term(Bin, [safe]),
            true = TXRoot2 == TXRoot,
            case get_block_entries(BlockStart, BlockEnd, DataRootIDs,
                                   ?DEFAULT_MODULE) of
                {ok, Entries} ->
                    {ok, {TXRoot, BlockSize, Entries}};
                Error ->
                    Error
            end
    end.

%% @doc Check the data root entries stored for the block starting at
%% BlockStart against its transaction paths, the way validate_data_roots/4
%% checks entries received from a peer. Return ok, not_found,
%% {invalid, Reason} if the entries do not match, or {error, Reason} if they
%% could not be read.
verify_stored_block(BlockStart, StoreID) ->
    maybe
        {ok, Bin} ?= get_keys(BlockStart, StoreID),
        {TXRoot, BlockSize, DataRootIDs} = binary_to_term(Bin, [safe]),
        {ok, Entries} ?= get_block_entries(BlockStart, BlockStart + BlockSize,
                                           DataRootIDs, StoreID),
        case verify_data_root_entries(Entries, TXRoot, BlockStart, BlockSize,
                                      0, 0) of
            ok ->
                ok;
            {error, Reason} ->
                {invalid, Reason}
        end
    end.

%% @doc Check up to Count stored blocks starting at or after Cursor and
%% before End with verify_stored_block/2. Remove the entries of the blocks
%% whose sizes and start offsets do not match their transaction paths, so that
%% data root syncing fetches them again, and keep the entries of blocks that
%% fail for another reason. Return {more | done, NextCursor, Checked,
%% Mismatches}, where Mismatches lists {BlockStart, BlockEnd, Reason,
%% removed | kept}, or {error, Reason} if no block could be checked.
verify_stored_blocks(Cursor, _End, 0, _StoreID) ->
    {more, Cursor, 0, []};
verify_stored_blocks(Cursor, End, Count, StoreID) ->
    case resume_block_removal(StoreID) of
        none ->
            verify_stored_blocks(Cursor, End, Count, StoreID, 0, []);
        {ok, BlockStart, BlockEnd} ->
            Mismatch = {BlockStart, BlockEnd, invalid_tx_start_offset, removed},
            verify_stored_blocks(Cursor, End, Count, StoreID, 1, [Mismatch]);
        {error, _} = Error ->
            Error
    end.

verify_stored_blocks(Cursor, _End, Count, _StoreID, Checked, Mismatches)
        when Checked == Count ->
    {more, Cursor, Checked, lists:reverse(Mismatches)};
verify_stored_blocks(Cursor, End, Count, StoreID, Checked, Mismatches) ->
    case ar_kv:get_next(keys_db(StoreID), << Cursor:?OFFSET_KEY_BITSIZE >>) of
        {ok, << BlockStart:?OFFSET_KEY_BITSIZE >>, _Bin}
                when BlockStart >= End ->
            {done, BlockStart, Checked, lists:reverse(Mismatches)};
        none ->
            {done, Cursor, Checked, lists:reverse(Mismatches)};
        {ok, << BlockStart:?OFFSET_KEY_BITSIZE >>, Bin} ->
            {_TXRoot, BlockSize, _DataRootIDs} = binary_to_term(Bin, [safe]),
            BlockEnd = BlockStart + BlockSize,
            case verify_and_remove_stored_block(BlockStart, BlockEnd,
                                                StoreID) of
                ok ->
                    verify_stored_blocks(BlockStart + 1, End, Count, StoreID,
                                         Checked + 1, Mismatches);
                {Action, Reason} when Action == removed; Action == kept ->
                    verify_stored_blocks(BlockStart + 1, End, Count, StoreID,
                                         Checked + 1,
                                         [{BlockStart, BlockEnd, Reason,
                                           Action} | Mismatches]);
                {error, _} = Error when Checked == 0 ->
                    Error;
                {error, _} ->
                    %% Report the progress made so far. The next call starts
                    %% at this block and returns the error.
                    {more, BlockStart, Checked, lists:reverse(Mismatches)}
            end;
        {error, _} = Error when Checked == 0 ->
            Error;
        {error, _} ->
            {more, Cursor, Checked, lists:reverse(Mismatches)}
    end.

%% @doc Remove the entries of the given stored block if their sizes and start
%% offsets do not match its transaction paths, keeping a copy in the migrations
%% index. Return ok, {removed, Reason}, {kept, Reason} or {error, Reason}.
%%
%% Peers could only make the node store entries whose sizes and start offsets
%% disagree with otherwise valid paths (invalid_tx_start_offset): every other
%% mismatch was already rejected when the entries were received, so entries
%% failing for another reason are kept for an operator to look into.
verify_and_remove_stored_block(BlockStart, BlockEnd, StoreID) ->
    case verify_stored_block(BlockStart, StoreID) of
        {invalid, invalid_tx_start_offset} ->
            remove_block(BlockStart, BlockEnd, StoreID);
        {invalid, Reason} ->
            {kept, Reason};
        {error, _} = Error ->
            Error;
        _ ->
            ok
    end.

%% @doc Back up a block's entries and journal their removal before deleting them.
remove_block(BlockStart, BlockEnd, StoreID) ->
    maybe
        {ok, Bin} ?= get_keys(BlockStart, StoreID),
        {TXRoot, BlockSize, DataRootIDs} = binary_to_term(Bin, [safe]),
        {ok, Entries} ?= get_block_entries(BlockStart, BlockEnd, DataRootIDs,
                                           StoreID),
        ok ?= ar_kv:put(ar_data_sync:migration_db(StoreID),
                        removed_block_key(BlockStart),
                        term_to_binary({TXRoot, BlockSize, Entries})),
        Pending = {entries, BlockStart, BlockEnd, Bin},
        ok ?= write_pending_removal(Pending, StoreID),
        {ok, BlockStart, BlockEnd} ?= do_remove_block(Pending, StoreID),
        {removed, invalid_tx_start_offset}
    else
        not_found ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc Resume an interrupted removal from its saved entries and phase. Name
%% the block in the error: verification cannot go on until the removal ends.
resume_block_removal(StoreID) ->
    case ar_kv:get(ar_data_sync:migration_db(StoreID), ?PENDING_REMOVAL_KEY) of
        not_found ->
            none;
        {ok, Bin} ->
            Pending = binary_to_term(Bin, [safe]),
            case do_remove_block(Pending, StoreID) of
                {error, Reason} ->
                    {error, {pending_removal, element(2, Pending), Reason}};
                Reply ->
                    Reply
            end;
        {error, _} = Error ->
            Error
    end.

do_remove_block({entries, BlockStart, BlockEnd, BlockKey}, StoreID) ->
    maybe
        {ok, Entries} ?= get_pending_entries(BlockStart, BlockEnd, BlockKey,
                                             StoreID),
        %% Keep the block key until every entry is gone so syncing skips it.
        %% Use exact keys: a neighboring block's forged start may be in this
        %% block's range, even though its proven end belongs to its own block.
        ok ?= remove_entries(Entries, StoreID),
        Pending = {block_key, BlockStart, BlockEnd, BlockKey},
        ok ?= write_pending_removal(Pending, StoreID),
        do_remove_block(Pending, StoreID)
    end;
do_remove_block({block_key, BlockStart, BlockEnd, BlockKey}, StoreID) ->
    maybe
        %% Once the key is deleted, syncing may store fresh entries. A retry
        %% must not delete entries again. Preserve a changed block key too.
        ok ?= remove_block_key(BlockStart, BlockKey, StoreID),
        ok ?= ar_kv:delete(ar_data_sync:migration_db(StoreID),
                           ?PENDING_REMOVAL_KEY),
        {ok, BlockStart, BlockEnd}
    end.

%% @doc Return the entries of a pending removal from its backup, or read them
%% again, restoring the backup, if it is gone and the journaled block key is
%% still stored: the key lists the entries the removal started from.
get_pending_entries(BlockStart, BlockEnd, BlockKey, StoreID) ->
    case get_removed_block(BlockStart, StoreID) of
        {ok, {_TXRoot, _BlockSize, Entries}} ->
            {ok, Entries};
        not_found ->
            case get_keys(BlockStart, StoreID) of
                {ok, BlockKey} ->
                    restore_removal_backup(BlockStart, BlockEnd, BlockKey,
                                           StoreID);
                {error, _} = Error ->
                    Error;
                _ ->
                    {error, missing_removal_backup}
            end;
        {error, _} = Error ->
            Error
    end.

restore_removal_backup(BlockStart, BlockEnd, BlockKey, StoreID) ->
    {TXRoot, BlockSize, DataRootIDs} = binary_to_term(BlockKey, [safe]),
    maybe
        {ok, Entries} ?= get_block_entries(BlockStart, BlockEnd, DataRootIDs,
                                           StoreID),
        ok ?= ar_kv:put(ar_data_sync:migration_db(StoreID),
                        removed_block_key(BlockStart),
                        term_to_binary({TXRoot, BlockSize, Entries})),
        {ok, Entries}
    end.

write_pending_removal(Pending, StoreID) ->
    ar_kv:put(ar_data_sync:migration_db(StoreID), ?PENDING_REMOVAL_KEY,
              term_to_binary(Pending)).

remove_block_key(BlockStart, BlockKey, StoreID) ->
    case get_keys(BlockStart, StoreID) of
        {ok, BlockKey} ->
            ar_kv:delete(keys_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>);
        {error, _} = Error ->
            Error;
        _ ->
            ok
    end.

remove_entries([], _StoreID) ->
    ok;
remove_entries([{DataRoot, TXSize, TXStart, _TXPath} | Entries], StoreID) ->
    case ar_kv:delete(index_db(StoreID),
                      index_key(DataRoot, TXSize, TXStart)) of
        ok ->
            remove_entries(Entries, StoreID);
        Error ->
            Error
    end.

removed_block_key(BlockStart) ->
    << ?REMOVED_BLOCK_KEY_PREFIX/binary, BlockStart:?OFFSET_KEY_BITSIZE >>.

%% @doc Return the entries remove_block/3 copied for the block starting at
%% BlockStart, as {ok, {TXRoot, BlockSize, Entries}}, or not_found.
get_removed_block(BlockStart, StoreID) ->
    case ar_kv:get(ar_data_sync:migration_db(StoreID),
                   removed_block_key(BlockStart)) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin, [safe])};
        Reply ->
            Reply
    end.

%% @doc Return the data root entries of the given IDs ending after BlockStart
%% and at or before BlockEnd, sorted by their end offsets, or {error, Reason}
%% if they could not be read. The end offsets, unlike the start offsets, are
%% proven by the transaction paths, so entries stored by earlier releases with
%% start offsets out of order or before the block are read too.
get_block_entries(BlockStart, BlockEnd, DataRootIDs, StoreID) ->
    Read = sets:fold(
             fun(_DataRootID, {error, _} = Error) ->
                     Error;
                (<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
                 {ok, Acc}) ->
                     case read_entries_in_range(DataRoot, TXSize,
                                                max(0, BlockStart - TXSize + 1),
                                                BlockEnd - TXSize + 1,
                                                StoreID) of
                         {ok, Entries} ->
                             {ok, Entries ++ Acc};
                         Error ->
                             Error
                     end
             end,
             {ok, []},
             DataRootIDs),
    case Read of
        {ok, Entries} ->
            {ok, lists:sort(
                   fun({_, TXSize1, TXStart1, _}, {_, TXSize2, TXStart2, _}) ->
                           TXStart1 + TXSize1 < TXStart2 + TXSize2
                   end,
                   Entries)};
        Error ->
            Error
    end.

%% @doc Validate the given data roots against the local block index.
%% Also recompute the TXRoot from entries and verify Merkle paths.
validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset) ->
    case ar_block_index:get_block_bounds(Offset) of
        not_found ->
            {error, not_found};
        Bounds ->
            validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset,
                Bounds)
    end.

%% @doc Validate the given data roots against the given block bounds.
validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset,
        {BlockStart, BlockEnd, ExpectedTXRoot}) ->
    maybe
        ok ?=
            case Offset >= BlockStart andalso Offset < BlockEnd of
                true ->
                    ok;
                false ->
                    {error, invalid_block_bounds}
            end,
        ok ?=
            case BlockSize == BlockEnd - BlockStart of
                true ->
                    ok;
                false ->
                    {error, invalid_block_size}
            end,
        ok ?=
            case TXRoot == ExpectedTXRoot of
                true ->
                    ok;
                false ->
                    {error, invalid_tx_root}
            end,
        ok ?= verify_data_root_entries(DataRootEntries, TXRoot, BlockStart, BlockSize, 0, 0),
        {ok, {TXRoot, BlockSize, DataRootEntries}}
    end.

%%%===================================================================
%%% Public: Entry lookup and iteration
%%%===================================================================
%% @doc Get the entry containing the given data root id.
get_entry(DataRootID, StoreID) ->
    << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
                                                % Since the index_db keys include the TX start offset, we have will stub in "a" (which is
                                                % guaranteed to be alphanumerically greater than any TX offset) and then query the
                                                % previous key.
    DataRootIndexKey = << DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
                          <<"a">>/binary >>,
    prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID).

iterator(DataRootID, TXStartOffset, StoreID) ->
    {DataRootID, TXStartOffset, TXStartOffset, StoreID, 1}.

next(Args) ->
    MaxDuplicates = arweave_config:get([gossip, data_roots, max_duplicates]),
    next(Args, MaxDuplicates).

next({_, _, _, _, Count}, Limit) when Count > Limit ->
    none;
next({_, 0, _, _, _}, _Limit) ->
    none;
next(Args, _Limit) ->
    {DataRootID, TXStartOffset, LatestTXStartOffset, StoreID, Count} = Args,
    case prev_entry(DataRootID, TXStartOffset, StoreID) of
        not_found ->
            none;
        {ok, {_DataRoot, _TXSize, TXStartOffset2, _TXPath} = Entry} ->
            {ok, Entry,
             {DataRootID, TXStartOffset2, LatestTXStartOffset, StoreID,
              Count + 1}};
        {error, _} = Error ->
            Error
    end.

reset({DataRootID, _, TXStartOffset, StoreID, _}) ->
    {DataRootID, TXStartOffset, TXStartOffset, StoreID, 1}.

id(Iterator) ->
    element(1, Iterator).

%%%===================================================================
%%% Public: Sync status and maintenance
%%%===================================================================
remove_range(Start, End, StoreID) ->
    {ok, RemovedDataRoots} =
        case ar_kv:get_range(keys_db(StoreID), << Start:?OFFSET_KEY_BITSIZE >>,
                             << (End - 1):?OFFSET_KEY_BITSIZE >>) of
            {ok, EmptyMap} when map_size(EmptyMap) == 0 ->
                {ok, sets:new()};
            {ok, Map} ->
                maps:fold(
                  fun
                      (_, _Value, {error, _} = Error) ->
                                 Error;
                      (_, Value, {ok, RemovedDataRoots}) ->
                                 {_TXRoot, _BlockSize, DataRootIDs} = binary_to_term(Value, [safe]),
                                 sets:fold(
                                   fun (_Key, {error, _} = Error) ->
                                           Error;
                                       (<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
                                        {ok, Removed}) ->
                                           case remove(DataRootID, Start, End, StoreID) of
                                               removed ->
                                                   {ok, sets:add_element(DataRootID, Removed)};
                                               ok ->
                                                   {ok, Removed};
                                               Error ->
                                                   Error
                                           end;
                                       (_, Acc) ->
                                           Acc
                                   end,
                                   {ok, RemovedDataRoots},
                                   DataRootIDs
                                  )
                         end,
                  {ok, sets:new()},
                  Map
                 );
            Error ->
                Error
        end,
    ok = ar_kv:delete_range(keys_db(StoreID),
                            << Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>),
    {ok, RemovedDataRoots}.

%% @doc Return true if the data roots for the given block range are synced, false otherwise.
are_synced(B, StoreID) ->
    BlockEnd = B#block.weave_size,
    BlockStart = BlockEnd - B#block.block_size,
    are_synced(BlockStart, BlockEnd, B#block.tx_root, StoreID).
are_synced(BlockStart, BlockEnd, TXRoot, StoreID) ->
    case get_keys(BlockStart, StoreID) of
        not_found ->
            false;
        {ok, Bin} ->
            {TXRoot2, BlockSize, _DataRootIDs} = binary_to_term(Bin, [safe]),
            TXRoot2 == TXRoot andalso BlockSize == BlockEnd - BlockStart
    end.

%% @doc Returns true if the given data root id is in the data root index.
is_synced(DataRootID, StoreID) ->
    case get_entry(DataRootID, StoreID) of
        {ok, _} ->
            true;
        _ ->
            false
    end.

%%%===================================================================
%%% Public: Repair
%%%===================================================================
repair(BI, StoreID, RemoveTXRangeFun) ->
    MigrationDB = ar_data_sync:migration_db(StoreID),
    case ar_kv:get(MigrationDB, <<"repair_data_root_offset_index">>) of
        not_found ->
            ?LOG_INFO([{event, starting_data_root_offset_index_scan}]),
            ReverseBI = lists:reverse(BI),
            ResyncBlocks = repair(ReverseBI, <<>>, 0, [], StoreID, RemoveTXRangeFun),
            ok = ar_kv:put(MigrationDB, <<"repair_data_root_offset_index">>, <<>>),
            ?LOG_INFO([{event, data_root_offset_index_scan_complete}]),
            {ok, ResyncBlocks};
        _ ->
            ok
    end.

%%%===================================================================
%%% Private: DB internals
%%%===================================================================
%% @doc Maintains a record of the data roots that have been synced and mapping of data roots
%% to TX start offsets and paths.
index_db(StoreID) ->
    {data_root_index, StoreID}.

get_keys(BlockStart, StoreID) ->
    ar_kv:get(keys_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>).

id(DataRoot, TXSize) ->
    << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>.

index_key(DataRoot, TXSize, TXStartOffset) ->
    << DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
       (ar_serialize:encode_int(TXStartOffset, 8))/binary >>.

parse_index_key(DataRootIndexKey) ->
    << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
       TXStartOffsetSize:8, TXStartOffset:(TXStartOffsetSize * 8) >> = DataRootIndexKey,
    {DataRoot, TXSize, TXStartOffset}.

%%%===================================================================
%%% Private: Entry lookup and range operations
%%%===================================================================
prev_entry(DataRootID, TXStartOffset, StoreID) ->
    << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
    DataRootIndexKey = index_key(DataRoot, TXSize, TXStartOffset - 1),
    prev_entry(DataRootIndexKey, StoreID).

prev_entry(DataRootIndexKey, StoreID) ->
    {DataRoot, TXSize, _TXStartOffset} = parse_index_key(DataRootIndexKey),
    prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID).

prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID) ->
    case ar_kv:get_prev(index_db(StoreID), DataRootIndexKey) of
        none ->
            not_found;
        {ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
                OffsetSize:8, TXStartOffset:(OffsetSize * 8) >>, TXPath} ->
            {ok, {DataRoot, TXSize, TXStartOffset, TXPath}};
        {ok, _, _} ->
            not_found;
        {error, _} = Error ->
            Error
    end.

remove(DataRootID, Start, End, StoreID) ->
    << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
    StartIndexKey = index_key(DataRoot, TXSize, Start),
    EndIndexKey = index_key(DataRoot, TXSize, End),
    case ar_kv:delete_range(index_db(StoreID), StartIndexKey, EndIndexKey) of
        ok ->
            case prev_entry(StartIndexKey, StoreID) of
                {ok, _} ->
                    ok;
                not_found ->
                    removed;
                {error, _} = Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @doc Get all the matching data root tuples matching DataRoom in the provided range
get_all_in_range(DataRoot, TXSize, Start, Cursor, StoreID) ->
    case read_entries_in_range(DataRoot, TXSize, Start, Cursor, StoreID) of
        {ok, Entries} ->
            Entries;
        {error, _} ->
            []
    end.

%% @doc Like get_all_in_range/5, but return {ok, Entries} or {error, Reason}
%% instead of hiding a read error behind an empty list.
read_entries_in_range(_DataRoot, _TXSize, Start, Cursor, _StoreID)
        when Cursor =< Start ->
    {ok, []};
read_entries_in_range(DataRoot, TXSize, Start, Cursor, StoreID) ->
    StartIndexKey = index_key(DataRoot, TXSize, Start),
    EndIndexKey = index_key(DataRoot, TXSize, Cursor - 1),
    case ar_kv:get_range(index_db(StoreID), StartIndexKey, EndIndexKey) of
        {ok, Range} ->
            DataRootEntries = maps:fold(
                                fun(DataRootIndexKey, TXPath, Acc) ->
                                        case parse_index_key(DataRootIndexKey) of
                                            {DataRoot, TXSize, TXStart} ->
                                                [{DataRoot, TXSize, TXStart, TXPath} | Acc];
                                            _ ->
                                                Acc
                                        end
                                end,
                                [],
                                Range
                               ),
            {ok, lists:sort(
                   fun({_DataRoot1, _TXSize1, TXStart1, _TXPath1},
                       {_DataRoot2, _TXSize2, TXStart2, _TXPath2}) ->
                           TXStart1 > TXStart2
                   end,
                   DataRootEntries
                  )};
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Private: Validation
%%%===================================================================
get_padded_size(TXSize, BlockStart) ->
    case BlockStart >= ar_block:strict_data_split_threshold() of
        true ->
            ar_poa:get_padded_offset(TXSize, 0);
        false ->
            TXSize
    end.

verify_data_root_entries([], _TXRoot, _BlockStart, BlockSize, Total, Total)
  when Total == BlockSize ->
    ok;
verify_data_root_entries([], _TXRoot, _BlockStart, _BlockSize, _TXStartOffset, _Total) ->
    {error, invalid_total_tx_size};
verify_data_root_entries([{_DataRoot, 0, _TXStartOffset, _TXPath} | _], _TXRoot, _BlockStart,
                         _BlockSize, _ExpectedTXStartOffset, _Total) ->
    {error, invalid_zero_tx_size};
verify_data_root_entries([{DataRoot, TXSize, TXStartOffset, TXPath} | DataRootEntries], TXRoot,
                         BlockStart, BlockSize, ExpectedTXStartOffset, Total) ->
    TXEndOffset = TXStartOffset + TXSize - BlockStart,
    case TXEndOffset >= 0 of
        false ->
            {error, invalid_entry_merkle_label};
        true ->
            case ar_merkle:validate_path(TXRoot, TXEndOffset - 1, BlockSize, TXPath) of
                false ->
                    {error, invalid_tx_path};
                {DataRoot, ExpectedTXStartOffset, TXEndOffset}
                        when TXStartOffset - BlockStart
                            == ExpectedTXStartOffset ->
                    PaddedTXSize = get_padded_size(TXSize, BlockStart),
                    PaddedEndOffset = get_padded_size(TXEndOffset, BlockStart),
                    verify_data_root_entries(
                      DataRootEntries,
                      TXRoot,
                      BlockStart,
                      BlockSize,
                      PaddedEndOffset,
                      Total + PaddedTXSize
                     );
                {DataRoot, ExpectedTXStartOffset, TXEndOffset} ->
                    %% The path proves where the transaction ends and, through
                    %% the previous entry, where it starts. A size and start
                    %% offset that only add up to the right end would index
                    %% the transaction under the wrong size.
                    {error, invalid_tx_start_offset};
                _ ->
                    {error, invalid_tx_path}
            end
    end.

%%%===================================================================
%%% Private: Repair
%%%===================================================================
repair(BI, Cursor, Height, ResyncBlocks, StoreID, RemoveTXRangeFun) ->
    case ar_kv:get_next(keys_db(StoreID), Cursor) of
        none ->
            ResyncBlocks;
        {ok, Key, Value} ->
            << BlockStart:?OFFSET_KEY_BITSIZE >> = Key,
            {TXRoot, BlockSize, _DataRootIDs} = binary_to_term(Value, [safe]),
            BlockEnd = BlockStart + BlockSize,
            case shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks, BI) of
                {ok, {Height2, BI2}} ->
                    Cursor2 = << (BlockStart + 1):?OFFSET_KEY_BITSIZE >>,
                    repair(BI2, Cursor2, Height2, ResyncBlocks, StoreID, RemoveTXRangeFun);
                {bad_key, []} ->
                    ResyncBlocks;
                {bad_key, ResyncBlocks2} ->
                    ?LOG_INFO([{event, removing_data_root_index_range},
                               {range_start, BlockStart}, {range_end, BlockEnd}]),
                    ok = RemoveTXRangeFun(BlockStart, BlockEnd),
                    {ok, _} = remove_range(BlockStart, BlockEnd, StoreID),
                    repair(BI, Cursor, Height, ResyncBlocks2, StoreID, RemoveTXRangeFun)
            end
    end.

shift_block_index(_TXRoot, _BlockStart, _BlockEnd, _Height, ResyncBlocks, []) ->
    {bad_key, ResyncBlocks};
shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks,
                  [{_H, WeaveSize, _TXRoot} | BI]) when BlockEnd > WeaveSize ->
    ResyncBlocks2 =
        case BlockStart < WeaveSize of
            true -> [Height | ResyncBlocks];
            false -> ResyncBlocks
        end,
    shift_block_index(TXRoot, BlockStart, BlockEnd, Height + 1, ResyncBlocks2, BI);
shift_block_index(TXRoot, _BlockStart, WeaveSize, Height, _ResyncBlocks,
                  [{_H, WeaveSize, TXRoot} | BI]) ->
    {ok, {Height + 1, BI}};
shift_block_index(_TXRoot, _BlockStart, _WeaveSize, Height, ResyncBlocks, _BI) ->
    {bad_key, [Height | ResyncBlocks]}.

%%%===================================================================
%%% Public: Block lifecycle helpers.
%%%===================================================================

%% @doc If the block's data roots aren't already indexed for this StoreID,
%% record them and update the per-tx offset/index. Idempotent.
add_block(B, SizeTaggedTXs, StoreID) ->
    #block{ indep_hash = H, weave_size = WeaveSize, tx_root = TXRoot } = B,
    case ar_block_index:get_element_by_height(B#block.height) of
        {H, WeaveSize, TXRoot} ->
            case are_synced(B, StoreID) of
                false ->
                    BlockStart = B#block.weave_size - B#block.block_size,
                    {ok, _} =
                        add_block_data_roots(SizeTaggedTXs, BlockStart, StoreID),
                    ok = update_tx_index(SizeTaggedTXs, BlockStart, StoreID),
                    ok;
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

%% @doc Update tx_index and tx_offset_index for the given block's transactions.
update_tx_index([], _BlockStartOffset, _StoreID) ->
    ok;
update_tx_index(SizeTaggedTXs, BlockStartOffset, StoreID) ->
    lists:foldl(
      fun ({_, Offset}, Offset) ->
              Offset;
          ({{padding, _}, Offset}, _) ->
              Offset;
          ({{TXID, _}, TXEndOffset}, PreviousOffset) ->
              AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
              TXSize = TXEndOffset - PreviousOffset,
              AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
              case ar_kv:put({tx_offset_index, StoreID},
                             << AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>, TXID) of
                  ok ->
                      case ar_kv:put({tx_index, StoreID}, TXID,
                                     term_to_binary({AbsoluteEndOffset, TXSize})) of
                          ok ->
                              ar_events:send(tx, {registered_offset, TXID, AbsoluteEndOffset,
                                                  TXSize}),
                              ar_tx_blacklist:notify_about_added_tx(TXID, AbsoluteEndOffset,
                                                                    AbsoluteStartOffset),
                              TXEndOffset;
                          {error, Reason} ->
                              ?LOG_ERROR([{event, failed_to_update_tx_index},
                                          {reason, io_lib:format("~p", [Reason])},
                                          {tx, arweave_util:encode(TXID)}]),
                              TXEndOffset
                      end;
                  {error, Reason} ->
                      ?LOG_ERROR([{event, failed_to_update_tx_offset_index},
                                  {reason, io_lib:format("~p", [Reason])},
                                  {tx, arweave_util:encode(TXID)}]),
                      TXEndOffset
              end
      end,
      0,
      SizeTaggedTXs
     ),
    ok.

%% @doc Run the data_root_offset_index repair scan for the given block
%% index and storage module. Removes tx_index ranges for any
%% inconsistent blocks discovered during the scan.
repair_data_root_offset_index(BI, StoreID) ->
    RemoveTXRange = fun(BlockStart, BlockEnd) ->
                            remove_tx_index_range(BlockStart, BlockEnd, StoreID)
                    end,
    case repair(BI, StoreID, RemoveTXRange) of
        {ok, ResyncBlocks} ->
            [ar_header_sync:remove_block(Height) || Height <- ResyncBlocks],
            ok;
        ok ->
            ok
    end.

%% @doc Remove tx_index / tx_offset_index entries for the given byte range.
%% Used during reorgs and on-startup orphan cleanup.
remove_tx_index_range(Start, End, StoreID) ->
    TXOffsetIndex = {tx_offset_index, StoreID},
    TXIndex = {tx_index, StoreID},
    ok = case ar_kv:get_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
                              << (End - 1):?OFFSET_KEY_BITSIZE >>) of
             {ok, EmptyMap} when map_size(EmptyMap) == 0 ->
                 ok;
             {ok, Map} ->
                 maps:fold(
                   fun
                       (_, _Value, {error, _} = Error) ->
                                  Error;
                       (_, TXID, ok) ->
                                  ar_kv:delete(TXIndex, TXID),
                                  ar_tx_blacklist:norify_about_orphaned_tx(TXID)
                          end,
                   ok,
                   Map
                  );
             Error ->
                 Error
         end,
    ar_kv:delete_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
                       << End:?OFFSET_KEY_BITSIZE >>).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ?LOG_INFO([{event, ar_data_roots_start}]),
    {ok, undefined}.

handle_call({store_block, BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID},
            _From, State) ->
    {ok, _} = store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({store_block, BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID}, State) ->
    {ok, _} = store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID),
    {noreply, State};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_DEBUG([{event, terminate}, {module, ?MODULE},
                {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_all_in_range_returns_multiple_matches_for_same_pair_test_() ->
    {timeout, 30, fun test_get_all_in_range_returns_multiple_matches_for_same_pair/0}.

test_get_all_in_range_returns_multiple_matches_for_same_pair() ->
    with_test_index_db(
      fun(StoreID) ->
              DataRoot = << 1:256 >>,
              TXSize = 100,
              ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 20, <<"path-20">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 30, <<"path-30">>),
              ?assertEqual(
                 [
                  {DataRoot, TXSize, 30, <<"path-30">>},
                  {DataRoot, TXSize, 20, <<"path-20">>},
                  {DataRoot, TXSize, 10, <<"path-10">>}
                 ],
                 get_all_in_range(DataRoot, TXSize, 10, 40, StoreID)
                )
      end
     ).

get_all_in_range_excludes_matches_outside_start_and_cursor_test_() ->
    {timeout, 30, fun test_get_all_in_range_excludes_matches_outside_start_and_cursor/0}.

test_get_all_in_range_excludes_matches_outside_start_and_cursor() ->
    with_test_index_db(
      fun(StoreID) ->
              DataRoot = << 2:256 >>,
              TXSize = 100,
              ok = put_test_tx(StoreID, DataRoot, TXSize, 5, <<"path-5">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 20, <<"path-20">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 30, <<"path-30">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize, 40, <<"path-40">>),
              ?assertEqual(
                 [
                  {DataRoot, TXSize, 30, <<"path-30">>},
                  {DataRoot, TXSize, 20, <<"path-20">>}
                 ],
                 get_all_in_range(DataRoot, TXSize, 15, 35, StoreID)
                )
      end
     ).

get_all_in_range_ignores_other_data_root_and_tx_size_test_() ->
    {timeout, 30, fun test_get_all_in_range_ignores_other_data_root_and_tx_size/0}.

test_get_all_in_range_ignores_other_data_root_and_tx_size() ->
    with_test_index_db(
      fun(StoreID) ->
              DataRoot = << 3:256 >>,
              OtherDataRoot = << 4:256 >>,
              TXSize = 100,
              ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
              ok = put_test_tx(StoreID, DataRoot, TXSize + 1, 20, <<"wrong-size">>),
              ok = put_test_tx(StoreID, OtherDataRoot, TXSize, 30, <<"wrong-root">>),
              ?assertEqual(
                 [{DataRoot, TXSize, 10, <<"path-10">>}],
                 get_all_in_range(DataRoot, TXSize, 0, 40, StoreID)
                )
      end
     ).

validate_data_roots_accepts_valid_entries_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, DataRootEntries} = make_valid_data_root_entries(BlockStart, [10, 20]),
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {ok, {TXRoot, BlockSize, DataRootEntries}},
                 validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset)
                )
      end
     ).

validate_data_roots_rejects_invalid_tx_path_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, [{DataRoot, TXSize, TXStartOffset, TXPath} | Rest]} =
        make_valid_data_root_entries(BlockStart, [10, 20]),
    InvalidEntries = [{DataRoot, TXSize, TXStartOffset, << TXPath/binary, 0 >>} | Rest],
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {error, invalid_tx_path},
                 validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
                )
      end
     ).

%% A size and start offset that keep the end offset the path proves, and the
%% block total, but split the transactions differently.
validate_data_roots_rejects_shifted_tx_start_offset_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, [{DataRoot1, 10, 0, TXPath1},
            {DataRoot2, 20, 10, TXPath2}]} =
        make_valid_data_root_entries(BlockStart, [10, 20]),
    ShiftedEntries = [{DataRoot1, 5, 5, TXPath1}, {DataRoot2, 25, 5, TXPath2}],
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {error, invalid_tx_start_offset},
                 validate_data_roots(TXRoot, BlockSize, ShiftedEntries, Offset)
                )
      end
     ).

%% The data doctor's check of stored blocks accepts entries built from the
%% block and reports entries stored with a shifted split.
verify_stored_block_test_() ->
    {timeout, 30, fun test_verify_stored_block/0}.

test_verify_stored_block() ->
    with_test_index_and_keys_db(
      fun(StoreID) ->
              {TXRoot, BlockSize, Entries} =
                  make_valid_data_root_entries(0, [10, 20, 30]),
              [{DataRoot1, 10, 0, TXPath1}, {DataRoot2, 20, 10, TXPath2},
               {DataRoot3, 30, 30, TXPath3}] = Entries,
              {ok, _} = store_block(0, BlockSize, TXRoot, Entries, StoreID),
              ?assertEqual(ok, verify_stored_block(0, StoreID)),
              %% The same block stored again at another offset, with the first
              %% transaction shrunk and the last one grown by the same amount.
              Start = 1000,
              Shifted = fun(S) ->
                  [{DataRoot1, 5, S + 5, TXPath1},
                   {DataRoot2, 20, S + 10, TXPath2},
                   {DataRoot3, 35, S + 25, TXPath3}]
              end,
              {ok, _} = store_block(Start, BlockSize, TXRoot, Shifted(Start),
                                    StoreID),
              ?assertEqual({invalid, invalid_tx_start_offset},
                           verify_stored_block(Start, StoreID)),
              ?assertEqual(not_found, verify_stored_block(2000, StoreID)),
              %% Check the stored blocks one at a time, below the offset of a
              %% third block, which is left unchecked.
              {ok, _} = store_block(2000, BlockSize, TXRoot, Shifted(2000),
                                    StoreID),
              ?assertEqual({more, 1, 1, []},
                           verify_stored_blocks(0, 2000, 1, StoreID)),
              ?assertEqual({more, Start + 1, 1,
                            [{Start, Start + BlockSize,
                              invalid_tx_start_offset, removed}]},
                           verify_stored_blocks(1, 2000, 1, StoreID)),
              ?assertEqual({done, 2000, 0, []},
                           verify_stored_blocks(Start + 1, 2000, 1, StoreID)),
              %% The mismatching block is removed, with a copy kept in the
              %% migrations index, and the others are kept.
              ?assertEqual(ok, verify_stored_block(0, StoreID)),
              ?assertEqual(not_found, verify_stored_block(Start, StoreID)),
              ?assertEqual({ok, {TXRoot, BlockSize, Shifted(Start)}},
                           get_removed_block(Start, StoreID)),
              ?assertEqual(not_found, get_removed_block(0, StoreID)),
              ?assertEqual({ok, []}, read_entries_in_range(DataRoot1, 5, Start,
                                                          Start + BlockSize,
                                                          StoreID)),
              ?assertEqual({invalid, invalid_tx_start_offset},
                           verify_stored_block(2000, StoreID)),
              ?assertEqual({done, 2000 + 1, 1,
                            [{2000, 2000 + BlockSize,
                              invalid_tx_start_offset, removed}]},
                           verify_stored_blocks(2000, 10000, 10, StoreID)),
              %% Entries failing for another reason, here a missing first
              %% transaction, are kept.
              Missing = [{DataRoot2, 20, 3000 + 10, TXPath2},
                         {DataRoot3, 30, 3000 + 30, TXPath3}],
              {ok, _} = store_block(3000, BlockSize, TXRoot, Missing, StoreID),
              ?assertMatch({done, _, 1, [{3000, _, invalid_tx_path, kept}]},
                           verify_stored_blocks(3000, 10000, 10, StoreID)),
              ?assertMatch({invalid, invalid_tx_path},
                           verify_stored_block(3000, StoreID))
      end).

%% Shifted entries the old validation accepted, in the order they were sent,
%% are removed even when their start offsets are out of transaction order or
%% before the block.
verify_stored_blocks_removes_unordered_shifted_entries_test_() ->
    {timeout, 30,
     fun test_verify_stored_blocks_removes_unordered_shifted_entries/0}.

test_verify_stored_blocks_removes_unordered_shifted_entries() ->
    with_test_index_and_keys_db(
      fun(StoreID) ->
              {TXRoot, BlockSize, Entries} =
                  make_valid_data_root_entries(0, [10, 20, 30]),
              [{DataRoot1, 10, 0, TXPath1}, {DataRoot2, 20, 10, TXPath2},
               {DataRoot3, 30, 30, TXPath3}] = Entries,
              %% The second transaction starts before the first one.
              Unordered = fun(S) ->
                  [{DataRoot1, 1, S + 9, TXPath1},
                   {DataRoot2, 29, S + 1, TXPath2},
                   {DataRoot3, 30, S + 30, TXPath3}]
              end,
              %% The first transaction starts before the block.
              BeforeBlock = fun(S) ->
                  [{DataRoot1, 15, S - 5, TXPath1},
                   {DataRoot2, 15, S + 15, TXPath2},
                   {DataRoot3, 30, S + 30, TXPath3}]
              end,
              lists:foreach(
                fun({Start, Shifted}) ->
                        ShiftedEntries = Shifted(Start),
                        {ok, _} = store_block(Start, BlockSize, TXRoot,
                                              ShiftedEntries, StoreID),
                        ?assertEqual({invalid, invalid_tx_start_offset},
                                     verify_stored_block(Start, StoreID)),
                        ?assertEqual({done, Start + 1, 1,
                                      [{Start, Start + BlockSize,
                                        invalid_tx_start_offset, removed}]},
                                     verify_stored_blocks(Start, Start + 1, 10,
                                                          StoreID)),
                        ?assertEqual(not_found,
                                     verify_stored_block(Start, StoreID)),
                        ?assertEqual([],
                                     [Entry || {DataRoot, TXSize, TXStart, _}
                                                   = Entry <- ShiftedEntries,
                                               ar_kv:get(index_db(StoreID),
                                                         index_key(DataRoot,
                                                                   TXSize,
                                                                   TXStart))
                                                   /= not_found])
                end,
                [{1000, Unordered}, {2000, BeforeBlock}])
      end).

%% A removal that fails halfway is resumed before data roots syncing can retry.
verify_stored_blocks_failed_removal_resyncs_test_() ->
    {timeout, 30, fun test_verify_stored_blocks_failed_removal_resyncs/0}.

test_verify_stored_blocks_failed_removal_resyncs() ->
    with_test_index_and_keys_db(
      fun(StoreID) ->
              {TXRoot, BlockSize, Entries} =
                  make_valid_data_root_entries(0, [10, 20, 30]),
              [{DataRoot1, 10, 0, TXPath1}, {DataRoot2, 20, 10, TXPath2},
               {DataRoot3, 30, 30, TXPath3}] = Entries,
              Start = 1000,
              End = Start + BlockSize,
              Shifted = [{DataRoot1, 5, Start + 5, TXPath1},
                         {DataRoot2, 20, Start + 10, TXPath2},
                         {DataRoot3, 35, Start + 25, TXPath3}],
              {ok, _} = store_block(Start, BlockSize, TXRoot, Shifted, StoreID),
              IndexDB = index_db(StoreID),
              FailingKey = index_key(DataRoot2, 20, Start + 10),
              Self = self(),
              ok = meck:new(ar_kv, [passthrough]),
              try
                  meck:expect(ar_kv, delete,
                      fun(DB, Key) when DB == IndexDB, Key == FailingKey ->
                              Self ! {synced_while_removing,
                                      are_synced(Start, End, TXRoot, StoreID)},
                              {error, injected};
                         (DB, Key) ->
                              meck:passthrough([DB, Key])
                      end),
                  ?assertEqual({error, injected},
                               verify_stored_blocks(Start, End, 10, StoreID))
              after
                  ok = meck:unload(ar_kv)
              end,
              %% The block key blocks syncing until the pending removal ends.
              ?assertEqual({synced_while_removing, true},
                           receive {synced_while_removing, _} = M -> M
                           after 0 -> timeout end),
              ?assertEqual(true, are_synced(Start, End, TXRoot, StoreID)),
              ?assertEqual({ok, {TXRoot, BlockSize, Shifted}},
                           get_removed_block(Start, StoreID)),
              ?assertMatch({done, _, 1, [{Start, End, _, removed}]},
                           verify_stored_blocks(Start, End, 10, StoreID)),
              ?assertEqual(false, are_synced(Start, End, TXRoot, StoreID)),
              ?assertEqual(not_found, get_entry(id(DataRoot3, 35), StoreID)),
              Synced = [{DataRoot1, 10, Start, TXPath1},
                        {DataRoot2, 20, Start + 10, TXPath2},
                        {DataRoot3, 30, Start + 30, TXPath3}],
              {ok, _} = store_block(Start, BlockSize, TXRoot, Synced, StoreID),
              ?assertEqual(true, are_synced(Start, End, TXRoot, StoreID)),
              ?assertEqual(ok, verify_stored_block(Start, StoreID))
      end).

%% Above the strict data split threshold every transaction is padded to whole
%% chunks, so an entry can be shortened within its last chunk without changing
%% the padded total. Only the start offset check rejects it.
verify_padded_entries_test() ->
    BlockStart = ar_block:strict_data_split_threshold(),
    {TXRoot, BlockSize, Entries} =
        make_padded_data_root_entries(BlockStart, [1000, 2000, 0, 300000]),
    ?assertEqual(4 * ?DATA_CHUNK_SIZE, BlockSize),
    ?assertMatch([{_, 1000, BlockStart, _},
                  {_, 2000, _, _},
                  {_, 300000, _, _}], Entries),
    ?assertEqual(ok, verify_data_root_entries(Entries, TXRoot, BlockStart,
                                              BlockSize, 0, 0)),
    [{DataRoot1, 1000, Start1, TXPath1} | Rest] = Entries,
    Shortened = [{DataRoot1, 999, Start1 + 1, TXPath1} | Rest],
    ?assertEqual({error, invalid_tx_start_offset},
                 verify_data_root_entries(Shortened, TXRoot, BlockStart,
                                          BlockSize, 0, 0)).

validate_data_roots_rejects_invalid_zero_tx_size_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, [{DataRoot, _TXSize, TXStartOffset, TXPath} | Rest]} =
        make_valid_data_root_entries(BlockStart, [10, 20]),
    InvalidEntries = [{DataRoot, 0, TXStartOffset, TXPath} | Rest],
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {error, invalid_zero_tx_size},
                 validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
                )
      end
     ).

validate_data_roots_rejects_invalid_total_tx_size_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, [Entry | _Rest]} =
        make_valid_data_root_entries(BlockStart, [10, 20]),
    InvalidEntries = [Entry],
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {error, invalid_total_tx_size},
                 validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
                )
      end
     ).

validate_data_roots_rejects_invalid_tx_root_test() ->
    BlockStart = 0,
    Offset = 1,
    {TXRoot, BlockSize, DataRootEntries} = make_valid_data_root_entries(BlockStart, [10, 20]),
    InvalidTXRoot = << 999:256 >>,
    with_mocked_block_bounds(
      BlockStart,
      BlockStart + BlockSize,
      TXRoot,
      fun() ->
              ?assertEqual(
                 {error, invalid_tx_root},
                 validate_data_roots(InvalidTXRoot, BlockSize, DataRootEntries, Offset)
                )
      end
     ).

with_test_index_db(Fun) ->
    ensure_test_kv_started(),
    Unique = integer_to_list(erlang:unique_integer([positive])),
    DBName = "ar_data_roots_test_" ++ Unique,
    StoreID = {ar_data_roots_test, Unique},
    ok = ar_kv:test_destroy(DBName),
    ok = open_index_db(ar_kv:test_db_path(), DBName, StoreID, []),
    try
        Fun(StoreID)
    after
        _ = ar_kv:test_close(index_db(StoreID)),
        ok = ar_kv:test_destroy(DBName)
    end.

with_test_index_and_keys_db(Fun) ->
    with_test_index_db(
      fun(StoreID) ->
              {ar_data_roots_test, Unique} = StoreID,
              KeysDBName = "ar_data_roots_test_keys_" ++ Unique,
              MigrationDBName = "ar_data_roots_test_migrations_" ++ Unique,
              ok = ar_kv:test_destroy(KeysDBName),
              ok = ar_kv:test_destroy(MigrationDBName),
              ok = ar_kv:open(#{
                     path => filename:join(ar_kv:test_db_path(), KeysDBName),
                     name => keys_db(StoreID) }),
              ok = ar_kv:open(#{
                     path => filename:join(ar_kv:test_db_path(),
                                           MigrationDBName),
                     name => ar_data_sync:migration_db(StoreID) }),
              try
                  Fun(StoreID)
              after
                  _ = ar_kv:test_close(keys_db(StoreID)),
                  _ = ar_kv:test_close(ar_data_sync:migration_db(StoreID)),
                  ok = ar_kv:test_destroy(KeysDBName),
                  ok = ar_kv:test_destroy(MigrationDBName)
              end
      end).

ensure_test_kv_started() ->
    case ar_kv_sup:start_link() of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

put_test_tx(StoreID, DataRoot, TXSize, TXStartOffset, TXPath) ->
    ar_kv:put(index_db(StoreID), index_key(DataRoot, TXSize, TXStartOffset), TXPath).

with_mocked_block_bounds(BlockStart, BlockEnd, TXRoot, Fun) ->
    meck:new(ar_block_index, [passthrough]),
    meck:expect(
      ar_block_index,
      get_block_bounds,
      fun(_) -> {BlockStart, BlockEnd, TXRoot} end
     ),
    try
        Fun()
    after
        ok = meck:unload(ar_block_index)
    end.

%% @doc Build the data root entries of a block placed after the 2.5 fork, where
%% each transaction is followed by a padding leaf up to the next chunk
%% boundary, the way ar_block:generate_size_tagged_list_from_txs/2 builds it.
make_padded_data_root_entries(BlockStart, TXSizes) ->
    {SizeTaggedTXs, _} =
        lists:foldl(
          fun(0, {Acc, End}) ->
                  {[{{dummy, << End:256 >>}, End} | Acc], End};
             (TXSize, {Acc, Start}) ->
                  End = Start + TXSize,
                  PaddedEnd = ar_poa:get_padded_offset(End, 0),
                  {[{{padding, ?PADDING_NODE_DATA_ROOT}, PaddedEnd},
                    {{dummy, << End:256 >>}, End} | Acc], PaddedEnd}
          end,
          {[], 0},
          TXSizes),
    {TXRoot, BlockSize, Entries} =
        build_block_data_root_entries(BlockStart, lists:reverse(SizeTaggedTXs)),
    {TXRoot, BlockSize, lists:reverse(Entries)}.

make_valid_data_root_entries(BlockStart, TXSizes) ->
    SizeTaggedTXs =
        lists:mapfoldl(
          fun(TXSize, TXEndOffset) ->
                  DataRoot = << TXEndOffset:256 >>,
                  NextTXEndOffset = TXEndOffset + TXSize,
                  {{{dummy, DataRoot}, NextTXEndOffset}, NextTXEndOffset}
          end,
          0,
          TXSizes
         ),
    {SizeTaggedTXs2, _} = SizeTaggedTXs,
    {TXRoot, BlockSize, DataRootEntriesReversed} =
        build_block_data_root_entries(BlockStart, SizeTaggedTXs2),
    DataRootEntries = lists:reverse(DataRootEntriesReversed),
    {TXRoot, BlockSize, DataRootEntries}.
