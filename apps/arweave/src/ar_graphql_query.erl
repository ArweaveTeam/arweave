-module(ar_graphql_query).

%% API
-export([execute/4]).

execute(Context, Object, Field, Args) ->
	try
		do_execute(Context, Object, Field, Args)
	catch
		exit:{timeout, {gen_server, call, [ar_sqlite3, _]}} ->
			{error, "ArQL unavailable."}
	end.

do_execute(_, _, <<"transaction">>, #{ <<"id">> := TXID }) ->
	case ar_sqlite3:select_tx_by_id(TXID) of
		{ok, TX} -> {ok, TX};
		not_found -> {ok, null}
	end;
do_execute(_, _, <<"transactions">>, Args) ->
	#{
		<<"from">> := FromQuery,
		<<"to">> := ToQuery,
		<<"tags">> := TagsQuery,
		<<"limit">> := LimitQuery,
		<<"offset">> := OffsetQuery
	} = Args,
	Opts = lists:concat([
		case FromQuery of
			From when is_list(From) -> [{from, From}];
			null -> []
		end,
		case ToQuery of
			To when is_list(To) -> [{to, To}];
			null -> []
		end,
		case TagsQuery of
			Tags when is_list(Tags) ->
				[{tags, lists:map(fun ar_graphql_tag:to_tuple/1, Tags)}];
			null ->
				[]
		end,
		case LimitQuery of
			Limit when is_integer(Limit) -> [{limit, Limit}];
			null -> []
		end,
		case OffsetQuery of
			Offset when is_integer(Offset) -> [{offset, Offset}];
			null -> []
		end
	]),
	{ok, [{ok, TX} || TX <- ar_sqlite3:select_txs_by(Opts)]};
do_execute(Ctx, Obj, <<"countTransactions">>, Args) ->
	{ok, Results} = execute(Ctx, Obj, <<"transactions">>, Args),
	{ok, length(Results)};
do_execute(_, _, <<"maxTransactionsLimit">>, _) ->
	{ok, ar_sqlite3:get_max_query_limit()}.
