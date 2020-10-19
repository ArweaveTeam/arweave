-module(ar_cleanup).

-export([remove_old_wallet_lists/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(KEEP_WALLET_LISTS, 1000).

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
