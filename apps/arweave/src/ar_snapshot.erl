-module(ar_snapshot).

-export([export/3, read_state/2, write/3, ensure_dir/1,
         block_time_history_bi/1, store_blocks/4, store_tx_header/2,
         store_wallet_list/4, tx_id/1]).

-include_lib("arweave/include/ar.hrl").

%%% Build start_from_state snapshot directories: the block index, the recent
%%% block and transaction headers, the reward and block time history entries
%%% and the account tree a node needs to join from a local state.

%% Mirrors ar_node_worker: how many missing recent block headers a node
%% tolerates when it starts from a state.
-ifndef(START_FROM_STATE_SEARCH_DEPTH).
-define(START_FROM_STATE_SEARCH_DEPTH, 100).
-endif.

%% The RocksDB databases of a snapshot directory as {Key, NodeDBName, DirName}:
%% NodeDBName is the name ar_storage opens the node's own copy under, DirName
%% the directory under rocksdb/.
-define(SNAPSHOT_DBS, [
    {tx_confirmation, tx_confirmation_db, "ar_storage_tx_confirmation_db"},
    {tx, tx_db, "ar_storage_tx_db"},
    {block, block_db, "ar_storage_block_db"},
    {reward_history, reward_history_db, "reward_history_db"},
    {block_time_history, block_time_history_db, "block_time_history_db"},
    {block_index, block_index_db, "block_index_db"},
    {account_tree, account_tree_db, "account_tree_db"}
]).

%%% The functions follow the export flow top-down; a helper with a single
%%% caller sits right below it. The helpers several of them share are at
%%% the end.

%% @doc Export the snapshot of the stopped node in DataDir with the block at
%% Height (or the latest block) as its tip into the new directory OutputDir.
export(DataDir, OutputDir, Height) ->
    case filelib:is_dir(filename:join(DataDir, ?ROCKS_DB_DIR)) of
        false ->
            {error, {data_dir_not_found, DataDir}};
        true ->
            export_from_dbs(DataDir, OutputDir, Height)
    end.

export_from_dbs(DataDir, OutputDir, Height) ->
    case open_source_dbs(DataDir) of
        {ok, Opened} ->
            Result = do_export(DataDir, OutputDir, Height),
            close_dbs(Opened),
            Result;
        {error, _} = Error ->
            Error
    end.

%% @doc Open the node databases of DataDir read-only under the names
%% ar_storage reads a start_from_state directory through.
open_source_dbs(DataDir) ->
    Specs = [{Key, ar_storage:get_db_name(Name, DataDir), DirName}
            || {Key, Name, DirName} <- ?SNAPSHOT_DBS],
    case open_dbs(Specs, filename:join(DataDir, ?ROCKS_DB_DIR), true) of
        {ok, DBs} ->
            {ok, maps:get(opened, DBs)};
        {error, _} = Error ->
            Error
    end.

do_export(DataDir, OutputDir, Height) ->
    maybe
        {ok, BI} ?= read_block_index(DataDir),
        {ok, BI2} ?= cut_block_index(BI, Height),
        {ok, State} ?= read_state(BI2, DataDir),
        ok ?= check_tip_readable(State),
        ok ?= ensure_dir(OutputDir),
        %% The tree of the tip's window must be present; do not fall back to
        %% an older tree and let the node join below the requested height.
        ok ?= write_new(OutputDir, State#{ search_depth => 1 }, DataDir),
        {ok, tip_info(State)}
    end.

read_block_index(DataDir) ->
    case ar_storage:read_block_index(DataDir) of
        not_found ->
            {error, block_index_not_found};
        BI ->
            {ok, BI}
    end.

cut_block_index(BI, latest) ->
    {ok, BI};
cut_block_index(BI, Height) when Height >= 0, Height < length(BI) ->
    {ok, lists:nthtail(length(BI) - 1 - Height, BI)};
cut_block_index(BI, Height) ->
    {error, {height_not_in_block_index, Height, length(BI) - 1}}.

check_tip_readable(#{ skipped := 0 }) ->
    ok;
check_tip_readable(#{ height := Height }) ->
    {error, {block_headers_not_found_above_height, Height}}.

tip_info(#{ height := Height, blocks := [B | _] }) ->
    #{ height => Height, hash => B#block.indep_hash,
       weave_size => B#block.weave_size }.

%% @doc Read the recent blocks and the history ranges the tip of BI needs
%% to start from a state, from the node's storage (not_set) or from the
%% snapshot directory CustomDir.
read_state(BI, CustomDir) ->
    Height = length(BI) - 1,
    SearchDepth = min(Height, ?START_FROM_STATE_SEARCH_DEPTH),
    case ar_node:read_recent_blocks(BI, SearchDepth, CustomDir) of
        not_found ->
            {error, block_headers_not_found};
        {Skipped, Blocks} ->
            ar:console("Snapshot: recent blocks ~B (skipped: ~B)~n",
                    [length(Blocks), Skipped]),
            BI2 = lists:nthtail(Skipped, BI),
            Height2 = Height - Skipped,
            {ok, #{
                block_index => BI,
                height => Height2,
                skipped => Skipped,
                search_depth => SearchDepth,
                blocks => Blocks,
                reward_history_bi =>
                    ar_rewards:interim_reward_history_bi(Height2, BI2),
                block_time_history_bi => block_time_history_bi(BI2)
            }}
    end.

%% @doc Return the part of BI the block time history is read for.
block_time_history_bi(BI) ->
    lists:sublist(BI, ar_block_time_history:history_length()
            + ar_block:get_consensus_window_size()).

%% @doc Create the snapshot directory; refuse to reuse an existing one.
ensure_dir(Dir) ->
    case file:read_file_info(Dir) of
        {ok, _} ->
            {error, {snapshot_dir_exists, Dir}};
        {error, enoent} ->
            filelib:ensure_dir(filename:join(Dir, "placeholder"));
        {error, Reason} ->
            {error, {snapshot_dir_unavailable, Reason}}
    end.

%% @doc Write the snapshot into the directory created for it, removing the
%% directory again when the write fails.
write_new(Dir, State, CustomDir) ->
    case write(Dir, State, CustomDir) of
        ok ->
            ok;
        {error, _} = Error ->
            _ = file:del_dir_r(Dir),
            Error
    end.

%% @doc Write the snapshot databases for State under Dir, reading the source
%% data from the node's storage (not_set) or the snapshot directory CustomDir.
write(Dir, State, CustomDir) ->
    case open_snapshot_dbs(Dir) of
        {ok, DBs} ->
            Result = do_write(State, CustomDir, DBs),
            close_dbs(maps:get(opened, DBs)),
            Result;
        {error, _} = Error ->
            Error
    end.

%% @doc Open fresh databases under Dir for writing a snapshot; the names must
%% not collide with the ones of a running node.
open_snapshot_dbs(Dir) ->
    Specs = [{Key, list_to_atom("snapshot_" ++ atom_to_list(Name)), DirName}
            || {Key, Name, DirName} <- ?SNAPSHOT_DBS],
    open_dbs(Specs, filename:join(Dir, ?ROCKS_DB_DIR), false).

do_write(State, CustomDir, DBs) ->
    #{ block_index := BI, blocks := Blocks, search_depth := SearchDepth,
       reward_history_bi := RewardHistoryBI,
       block_time_history_bi := BlockTimeHistoryBI } = State,
    ReadWalletList = fun(RootHash) ->
        ar_storage:read_wallet_list(RootHash, CustomDir)
    end,
    maybe
        ok ?= write_block_index(BI, maps:get(block_index, DBs)),
        {ok, TXs} ?= store_blocks(Blocks, CustomDir, DBs, "Snapshot"),
        ok ?= store_tx_headers(TXs, CustomDir, maps:get(tx, DBs)),
        ok ?= copy_history(RewardHistoryBI, reward_history_db, CustomDir,
                maps:get(reward_history, DBs)),
        ok ?= copy_history(BlockTimeHistoryBI, block_time_history_db,
                CustomDir, maps:get(block_time_history, DBs)),
        ok ?= store_wallet_list(Blocks, SearchDepth, ReadWalletList,
                maps:get(account_tree, DBs)),
        verify(BI, Blocks, DBs)
    end.

%% @doc Store BI (newest first) keyed by height as {H, WeaveSize, TXRoot,
%% PrevH}, the layout ar_storage:read_block_index/1 reads.
write_block_index(BI, DB) ->
    write_block_index(lists:reverse(BI), 0, <<>>, DB).

write_block_index([], _Height, _PrevH, _DB) ->
    ok;
write_block_index([{H, WeaveSize, TXRoot} | BI], Height, PrevH, DB) ->
    Bin = term_to_binary({H, WeaveSize, TXRoot, PrevH}),
    case ar_kv:put(DB, << Height:256 >>, Bin) of
        ok ->
            write_block_index(BI, Height + 1, H, DB);
        Error ->
            Error
    end.

%% @doc Store the block headers and their transaction confirmations in the
%% block and tx_confirmation databases of TargetDBs; the confirmations are
%% read from the tx_confirmation_db of the node's storage (not_set) or of
%% the snapshot directory CustomDir. Return {ok, TXs} with the distinct
%% transactions of the blocks.
store_blocks(Blocks, CustomDir, TargetDBs, LogPrefix) ->
    #{ block := BlockDB, tx_confirmation := TXConfirmationDB } = TargetDBs,
    SourceDB = ar_storage:get_db_name(tx_confirmation_db, CustomDir),
    case store_blocks(Blocks, SourceDB, BlockDB, TXConfirmationDB, #{}) of
        {ok, TXs} ->
            ar:console("~s: stored ~B block headers and ~B transaction "
                    "confirmations~n",
                    [LogPrefix, length(Blocks), length(TXs)]),
            {ok, TXs};
        {error, _} = Error ->
            Error
    end.

store_blocks([], _SourceDB, _BlockDB, _TXConfirmationDB, TXMap) ->
    {ok, maps:values(TXMap)};
store_blocks([B | Blocks], SourceDB, BlockDB, TXConfirmationDB, TXMap) ->
    #block{ indep_hash = H, height = Height, txs = TXs } = B,
    maybe
        ok ?= store_block(B, BlockDB),
        ok ?= copy_tx_confirmations(TXs, Height, H, SourceDB,
                TXConfirmationDB),
        BlockTXs = [{tx_id(TX), TX} || TX <- TXs],
        TXMap2 = maps:merge(TXMap, maps:from_list(BlockTXs)),
        store_blocks(Blocks, SourceDB, BlockDB, TXConfirmationDB, TXMap2)
    end.

%% @doc Store the block header with its transactions replaced by their
%% identifiers, keyed by the block hash.
store_block(B, DB) ->
    TXIDs = [tx_id(TX) || TX <- B#block.txs],
    Bin = ar_serialize:block_to_binary(B#block{ txs = TXIDs }),
    ar_kv:put(DB, B#block.indep_hash, Bin).

%% @doc Copy the confirmation entries of the block's transactions, building
%% the entry from the block height and hash when the source has none.
copy_tx_confirmations([], _Height, _H, _SourceDB, _DB) ->
    ok;
copy_tx_confirmations([TX | TXs], Height, H, SourceDB, DB) ->
    TXID = tx_id(TX),
    Result =
        case ar_kv:get(SourceDB, TXID) of
            {ok, Bin} ->
                ar_kv:put(DB, TXID, Bin);
            not_found ->
                ar_kv:put(DB, TXID, term_to_binary({Height, H}));
            {error, _} = Error ->
                Error
        end,
    case Result of
        ok ->
            copy_tx_confirmations(TXs, Height, H, SourceDB, DB);
        Error2 ->
            Error2
    end.

store_tx_headers([], _CustomDir, _DB) ->
    ok;
store_tx_headers([TX | TXs], CustomDir, DB) ->
    case ar_storage:read_tx(TX, CustomDir) of
        #tx{} = TX2 ->
            case store_tx_header(TX2, DB) of
                ok ->
                    store_tx_headers(TXs, CustomDir, DB);
                Error ->
                    Error
            end;
        _ ->
            {error, {tx_unavailable, arweave_util:encode(tx_id(TX))}}
    end.

%% @doc Store the header of TX in DB, keeping the data of format 1
%% transactions inline and dropping the data of format 2 ones.
store_tx_header(#tx{ format = 1 } = TX, DB) ->
    ar_kv:put(DB, TX#tx.id, ar_serialize:tx_to_binary(TX));
store_tx_header(TX, DB) ->
    TX2 = TX#tx{ data = <<>> },
    ar_kv:put(DB, TX#tx.id, ar_serialize:tx_to_binary(TX2)).

%% @doc Copy the entries of the blocks in HistoryBI from the node's database
%% NodeDBName (or its copy under CustomDir) to DB.
copy_history(HistoryBI, NodeDBName, CustomDir, DB) ->
    SourceDB = ar_storage:get_db_name(NodeDBName, CustomDir),
    copy_history(HistoryBI, NodeDBName, SourceDB, DB, ok).

copy_history(_HistoryBI, _NodeDBName, _SourceDB, _DB, {error, _} = Error) ->
    Error;
copy_history([], _NodeDBName, _SourceDB, _DB, ok) ->
    ok;
copy_history([{H, _, _} | BI], NodeDBName, SourceDB, DB, ok) ->
    Result =
        case ar_kv:get(SourceDB, H) of
            {ok, Bin} ->
                ar_kv:put(DB, H, Bin);
            not_found ->
                {error, {NodeDBName, not_found, arweave_util:encode(H)}};
            {error, Reason} ->
                {error, {NodeDBName, Reason, arweave_util:encode(H)}}
        end,
    copy_history(BI, NodeDBName, SourceDB, DB, Result).

%% @doc Store the account tree of the oldest consensus-window block of Blocks
%% in DB, dropping up to SearchDepth - 1 tips whose window has no readable
%% tree; ReadWalletList(RootHash) returns {ok, Tree} for a stored tree.
store_wallet_list(Blocks, SearchDepth, ReadWalletList, DB) ->
    case find_wallet_tree(Blocks, SearchDepth, 0, ReadWalletList) of
        {ok, {B, Tree}} ->
            {RootHash, _Tree2, UpdateMap} = ar_block:hash_wallet_list(Tree),
            ar:console("Snapshot: account tree ~s height ~B~n",
                    [arweave_util:encode(RootHash), B#block.height]),
            case RootHash == B#block.wallet_list of
                true ->
                    store_account_tree(UpdateMap, DB);
                false ->
                    {error, {wallet_list_root_mismatch, RootHash,
                            B#block.wallet_list}}
            end;
        not_found ->
            B = window_base_block(Blocks),
            {error, {account_tree_not_found, B#block.height,
                    arweave_util:encode(B#block.wallet_list)}}
    end.

find_wallet_tree([], _SearchDepth, _Skipped, _ReadWalletList) ->
    not_found;
find_wallet_tree(_Blocks, Skipped, Skipped, _ReadWalletList) ->
    not_found;
find_wallet_tree(Blocks, SearchDepth, Skipped, ReadWalletList) ->
    B = window_base_block(Blocks),
    case ReadWalletList(B#block.wallet_list) of
        {ok, Tree} ->
            {ok, {B, Tree}};
        _ ->
            find_wallet_tree(tl(Blocks), SearchDepth, Skipped + 1,
                    ReadWalletList)
    end.

%% @doc Store the account tree nodes of the update map, keyed like
%% ar_storage stores them; existing entries are left as they are.
store_account_tree(UpdateMap, DB) ->
    maps:fold(
        fun(_Key, _Value, {error, _} = Error) ->
                Error;
           ({H, Prefix}, Value, ok) ->
                Prefix2 = case Prefix of root -> <<>>; _ -> Prefix end,
                DBKey = << H/binary, Prefix2/binary >>,
                case ar_kv:get(DB, DBKey) of
                    not_found ->
                        ar_kv:put(DB, DBKey, term_to_binary(Value));
                    {ok, _} ->
                        ok;
                    {error, Reason} ->
                        {error, {account_tree_read_failed, Reason}}
                end
        end,
        ok,
        UpdateMap
    ).

%% @doc Check the written snapshot: the block index reads back as written,
%% every recent block header is stored and the account tree root of one of
%% the blocks is present.
verify(BI, Blocks, DBs) ->
    maybe
        ok ?= verify_block_index(BI, maps:get(block_index, DBs)),
        ok ?= verify_blocks(Blocks, maps:get(block, DBs)),
        verify_account_tree_root(Blocks, maps:get(account_tree, DBs))
    end.

verify_block_index(BI, DB) ->
    case read_stored_block_index(DB) of
        BI ->
            ok;
        not_found ->
            {error, snapshot_block_index_not_found};
        _ ->
            {error, snapshot_block_index_mismatch}
    end.

read_stored_block_index(DB) ->
    case ar_kv:get_prev(DB, <<"a">>) of
        none ->
            not_found;
        {ok, << Height:256 >>, _Value} ->
            {ok, Map} = ar_kv:get_range(DB, << 0:256 >>, << Height:256 >>),
            read_stored_block_index(Map, 0, Height, [])
    end.

read_stored_block_index(_Map, Height, End, BI) when Height > End ->
    BI;
read_stored_block_index(Map, Height, End, BI) ->
    case maps:find(<< Height:256 >>, Map) of
        {ok, Bin} ->
            {H, WeaveSize, TXRoot, _PrevH} = binary_to_term(Bin, [safe]),
            read_stored_block_index(Map, Height + 1, End,
                    [{H, WeaveSize, TXRoot} | BI]);
        error ->
            not_found
    end.

verify_blocks([], _DB) ->
    ok;
verify_blocks([B | Blocks], DB) ->
    H = B#block.indep_hash,
    case ar_kv:get(DB, H) of
        {ok, _} ->
            verify_blocks(Blocks, DB);
        not_found ->
            {error, {snapshot_block_missing, arweave_util:encode(H)}};
        {error, _} = Error ->
            Error
    end.

verify_account_tree_root([], _DB) ->
    {error, wallet_list_not_found};
verify_account_tree_root([B | Blocks], DB) ->
    case ar_kv:get(DB, B#block.wallet_list) of
        {ok, _} ->
            ok;
        _ ->
            verify_account_tree_root(Blocks, DB)
    end.

%%%===================================================================
%%% Shared helpers.
%%%===================================================================

%% @doc Open every {Key, Name, DirName} database under RocksDir; return
%% {ok, DBs} with the names keyed by Key plus the opened list, closing the
%% already opened databases when one fails.
open_dbs(Specs, RocksDir, ReadOnly) ->
    open_dbs(Specs, RocksDir, ReadOnly, #{}, []).

open_dbs([], _RocksDir, _ReadOnly, DBs, Opened) ->
    {ok, DBs#{ opened => Opened }};
open_dbs([{Key, Name, DirName} | Specs], RocksDir, ReadOnly, DBs, Opened) ->
    Path = filename:join(RocksDir, DirName),
    case ar_kv:open(#{ path => Path, name => Name, readonly => ReadOnly }) of
        ok ->
            open_dbs(Specs, RocksDir, ReadOnly, DBs#{ Key => Name },
                    [Name | Opened]);
        {error, Reason} ->
            close_dbs(Opened),
            {error, {open_db_failed, Path, Reason}}
    end.

close_dbs(Names) ->
    lists:foreach(fun ar_kv:close/1, Names).

%% @doc Return the oldest block of the consensus window that starts at the
%% head of Blocks, or the last block when there are fewer.
window_base_block(Blocks) ->
    WindowSize = ar_block:get_consensus_window_size(),
    case length(Blocks) >= WindowSize of
        true ->
            lists:nth(WindowSize, Blocks);
        false ->
            lists:last(Blocks)
    end.

%% @doc Return the identifier of a transaction record or identifier.
tx_id(#tx{ id = TXID }) ->
    TXID;
tx_id(TXID) ->
    TXID.
