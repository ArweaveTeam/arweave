-module(ar_cleanup).

-export([rewrite/0, rewrite/1, remove_old_wallet_lists/0, cleanup_blocks_on_disck/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(KEEP_WALLET_LISTS, 1000).

%% @doc Rewrite every block in the hash list using the latest format.
%% In the case of upgrading a node from 1.1 to 1.5, this dramatically reduces
%% the size of the weave on disk (and on the wire).
rewrite() ->
	rewrite(lists:reverse(ar_node:get_block_index(whereis(http_entrypoint_node)))).
rewrite(BI) -> rewrite(BI, BI).
rewrite([], _BI) -> [];
rewrite([{H, _, _} | Rest], BI) ->
	try ar_storage:read_block(H) of
		B when ?IS_BLOCK(B) ->
			ar_storage:write_block(B),
			ar:report([{rewrote_block, ar_util:encode(H)}]);
		unavailable ->
			do_nothing
	catch _:_ ->
		ar:report([{error_rewriting_block, ar_util:encode(H)}])
	end,
	rewrite(Rest, BI).

remove_old_wallet_lists() ->
	DataDir = rpc:call('arweave@127.0.0.1', ar_meta_db, get, [data_dir], 5000),
	WalletListDir = filename:join(DataDir, ?WALLET_LIST_DIR),
	case file:list_dir(WalletListDir) of
		{ok, Filenames} ->
			WalletListFilepaths = lists:filtermap(
				fun(Filename) ->
					case string:split(Filename, ".json") of
						[Hash, []] when length(Hash) > 0 ->
							{true, filename:join(WalletListDir, Filename)};
						_ ->
							false
					end
				end,
				Filenames
			),
			remove_old_wallet_lists(WalletListFilepaths);
		{error, Reason} ->
			io:format("~nFailed to scan the disk, reason ~p", [Reason]),
			erlang:halt(1)
	end.

remove_old_wallet_lists(Filepaths) when length(Filepaths) =< ?KEEP_WALLET_LISTS ->
	io:format("~nCurrently less than ~B wallets on disk, nothing to clean.~n~n", [?KEEP_WALLET_LISTS]),
	erlang:halt(0);
remove_old_wallet_lists(Filepaths) ->
	SortedFilepaths =
		lists:sort(
			fun(A, B) -> filelib:last_modified(A) < filelib:last_modified(B) end,
			Filepaths
		),
	ToRemove = lists:sublist(SortedFilepaths, length(Filepaths) - ?KEEP_WALLET_LISTS),
	lists:foreach(
		fun(File) ->
			case file:delete(File) of
				ok ->
					ok;
				{error, Reason} ->
					io:format("~nFailed to remove file ~s for reason ~p.~n~n", [File, Reason])
			end
		end,
		ToRemove
	),
	io:format("~nCleanup complete.~n~n"),
	erlang:halt(0).

cleanup_blocks_on_disck(Block) ->
	case ar_meta_db:get(used_space) >= ar_meta_db:get(disk_space) of
		true ->
			ReverseBI = lists:reverse(ar_node:get_block_index(whereis(http_entrypoint_node))),
			BlockSize = byte_size(ar_serialize:jsonify(ar_serialize:block_to_json_struct(Block))),
			ok = cleanup_by_paths(ReverseBI, BlockSize, 0);
		false ->
			ok
	end.

cleanup_by_paths([], _, _) ->
	ok;
cleanup_by_paths([{BH, _, _}|T], CurrentSize, Size) ->
	case CurrentSize > Size of
		true ->
			case ar_storage:lookup_block_filename(BH) of
				unavailable ->
					cleanup_by_paths(T, CurrentSize, Size);
				BlockPath ->
					case ar_storage:read_block(BH) of
						unavailable ->
							cleanup_by_paths(T, CurrentSize, Size);
						#block{ txs = TXs, wallet_list_hash = WalletListHash } ->
							_ = [file:delete(ar_storage:lookup_tx_filename(TX)) || TX <- TXs],
							ok = file:delete(ar_storage:wallet_list_filepath(WalletListHash)),
							ok = file:delete(BlockPath),
							cleanup_by_paths(T, CurrentSize, Size + filelib:file_size(BlockPath))
					end
				end;
		false ->
			ok
	end.
