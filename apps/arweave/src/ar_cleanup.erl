-module(ar_cleanup).

-export([rewrite/0, rewrite/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Rewrite every block in the hash list using the latest format.
%% In the case of upgrading a node from 1.1 to 1.5, this dramatically reduces
%% the size of the weave on disk (and on the wire).
rewrite() ->
	rewrite(lists:reverse(ar_node:get_block_index(whereis(http_entrypoint_node)))).
rewrite(BI) -> rewrite(BI, BI).
rewrite([], _BI) -> [];
rewrite([{H, _, _} | Rest], BI) ->
	try ar_storage:read_block(H, BI) of
		B when ?IS_BLOCK(B) ->
			ar_storage:write_block(B),
			ar:report([{rewrote_block, ar_util:encode(H)}]);
		unavailable ->
			do_nothing
	catch _:_ ->
		ar:report([{error_rewriting_block, ar_util:encode(H)}])
	end,
	rewrite(Rest, BI).
