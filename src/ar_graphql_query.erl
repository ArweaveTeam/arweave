-module(ar_graphql_query).

-include("ar_graphql.hrl").

%% API
-export([execute/4]).

execute(_, _, <<"transaction">>, #{ <<"id">> := TXID }) ->
	case ar_graphql_transaction:from_txid(TXID) of
		#ar_graphql_transaction {} = Tx ->
			{ok, Tx};
		undefined ->
			{ok, null}
	end;
execute(_, _, <<"transactions">>, #{ <<"tags">> := Tags }) ->
	TagMatchSet = ar_graphql_transaction:tag_match_set(Tags),
	TXIDs = sets:to_list(TagMatchSet),
	SortedTXIDs = ar_tx_search:sort_txids(TXIDs),
	{ok, [{ok, #ar_graphql_transaction { id = ar_util:encode(TXID) }} || TXID <- SortedTXIDs]};
execute(Ctx, Obj, <<"countTransactions">>, Args) ->
	{ok, Results} = execute(Ctx, Obj, <<"transactions">>, Args),
	{ok, length(Results)}.
