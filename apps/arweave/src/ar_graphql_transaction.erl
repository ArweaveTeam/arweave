-module(ar_graphql_transaction).

%% API
-export([execute/4]).

execute(_, #{ id := TXID }, <<"id">>, #{}) ->
	{ok, TXID};
execute(_, #{ id := TXID }, <<"tags">>, #{}) ->
	Tags = ar_sqlite3:select_tags_by_tx_id(TXID),
	{ok, [{ok, Tag} || Tag <- prune_non_unicode_tags(Tags)]};
execute(_, #{ id := TXID }, <<"tagValue">>, #{ <<"tagName">> := TagName }) ->
	Tags = ar_sqlite3:select_tags_by_tx_id(TXID),
	case lists:search(
		fun(#{ name := Name }) -> Name == TagName end,
		prune_non_unicode_tags(Tags)
	) of
		{value, #{ value := Value }} -> {ok, Value};
		false -> {ok, null}
	end;
execute(_, #{ id := TXID }, <<"linkedToTransaction">>, #{ <<"byOwnTag">> := OwnTagName }) ->
	Tags = ar_sqlite3:select_tags_by_tx_id(TXID),
	case lists:search(fun(#{ name := Name }) -> Name == OwnTagName end, Tags) of
		{value, #{ value := Value }} ->
			case ar_sqlite3:select_tx_by_id(Value) of
				{ok, Tx} -> {ok, Tx};
				not_found -> {ok, null}
			end;
		false ->
			{ok, null}
	end;
execute(_, #{ id := TXID }, <<"linkedFromTransactions">>, Args) ->
	#{
		<<"byForeignTag">> := ForeignTagName,
		<<"from">> := FromQuery,
		<<"to">> := ToQuery,
		<<"tags">> := TagsQuery,
		<<"limit">> := LimitQuery,
		<<"offset">> := OffsetQuery
	} = Args,
	Tags = case TagsQuery of
		List when is_list(List) ->
			List;
		null ->
			[]
	end,
	Opts = lists:concat([
		case FromQuery of
			From when is_list(From) -> [{from, From}];
			null -> []
		end,
		case ToQuery of
			To when is_list(To) -> [{to, To}];
			null -> []
		end,
		[{tags, [{ForeignTagName, TXID} | lists:map(fun ar_graphql_tag:to_tuple/1, Tags)]}],
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
execute(Ctx, Obj, <<"countLinkedFromTransactions">>, Args) ->
	{ok, Results} = execute(Ctx, Obj, <<"linkedFromTransactions">>, Args),
	{ok, length(Results)}.

prune_non_unicode_tags(Tags) ->
	lists:filter(fun(#{ name := Name, value := Value }) ->
		UnicodeName = unicode:characters_to_binary(Name),
		UnicodeValue = unicode:characters_to_binary(Value),
		is_binary(UnicodeName) andalso is_binary(UnicodeValue)
	end, Tags).
