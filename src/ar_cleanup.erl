-module(ar_cleanup).
-export([remove_invalid_blocks/1]).
-export([rewrite/0, rewrite/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%
%%% @doc Functions to clean up blocks not on the list of valid blocks
%%% and invalid transactions that are no longer valid for some reason.
%%%

%% @doc Remove all blocks from blocks directory not in HashList
remove_invalid_blocks(HashList) ->
	{ok, RawFiles} = file:list_dir(?BLOCK_DIR),
	Files =
		lists:filter(
			fun(X) ->
				not filelib:is_dir(X)
			end,
			RawFiles
		),
	lists:foreach(
		fun(X) ->
			file:delete(?BLOCK_DIR ++ X)
		end,
		lists:filter(
			fun(Y) ->
				case lists:foldl(
					fun(Z, Sum) -> Sum + string:str(Y, binary_to_list(ar_util:encode(Z))) end,
					0,
					HashList
				) of
					0 -> true;
					_ -> false
				end
			end,
			Files
		)
	),
	case file:list_dir(?BLOCK_ENC_DIR) of
		{ok, FilesEnc} ->
			lists:foreach(
				fun(X) ->
					file:delete(?BLOCK_ENC_DIR ++ X)
				end,
				FilesEnc
			);
		_ -> do_nothing
	end.

%% @doc Rewrite every block in the hash list using the latest format.
%% In the case of upgrading a node from 1.1 to 1.5, this dramatically reduces
%% the size of the weave on disk (and on the wire).
rewrite() ->
	rewrite(ar_node:get_hash_list(whereis(http_entrypoint_node))).
rewrite(BHL) -> rewrite(BHL, BHL).
rewrite([], _BHL) -> [];
rewrite([H|Rest], BHL) ->
	try ar_storage:read_block(H, BHL) of
		B when ?IS_BLOCK(B) ->
			ar_storage:write_block(B),
			ar:report([{compacted_block, ar_util:encode(H)}]);
		unavailable ->
			do_nothing
	catch _:_ ->
		ar:report([{error_compacting_block, ar_util:encode(H)}])
	end,
	rewrite(Rest, BHL).

%%%
%%% Tests.
%%%

%% @doc Remove all TXs from the TX directory that are "too cheap"
remove_block_keep_directory_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	remove_invalid_blocks([]),
	{ok, Files} = (file:list_dir(?BLOCK_DIR)),
	0 = length(lists:filter(fun filelib:is_file/1, Files -- [".gitignore"])).

%%%
%%% EOF
%%%
