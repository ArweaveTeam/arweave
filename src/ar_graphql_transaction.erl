-module(ar_graphql_transaction).

-include("ar_graphql.hrl").

%% API
-export([execute/4]).

%% Utility functions
-export([from_txid/1, tag_match_set/1]).

execute(_, #ar_graphql_transaction { id = TXID }, <<"id">>, #{}) ->
	{ok, TXID};
execute(_, Tx, <<"tags">>, #{}) ->
	#ar_graphql_transaction { tags = Tags } = load_tags(Tx),
	{ok, [{ok, Tag} || Tag <- Tags]};
execute(_, Tx, <<"tagValue">>, #{ <<"tagName">> := Name }) ->
	#ar_graphql_transaction { tags = Tags } = load_tags(Tx),
		case lists:keyfind(Name, #ar_graphql_tag.name, Tags) of
		#ar_graphql_tag{ value = Value } ->
			{ok, Value};
		false ->
			{ok, null}
	end;
execute(_, Tx, <<"linkedToTransaction">>, #{ <<"byOwnTag">> := OwnTagName }) ->
	#ar_graphql_transaction { tags = OwnTags } = load_tags(Tx),
	case lists:keyfind(OwnTagName, #ar_graphql_tag.name, OwnTags) of
		#ar_graphql_tag{ value = Value } ->
			case from_txid(Value) of
				#ar_graphql_transaction {} = LinkedTx ->
					{ok, LinkedTx};
				undefined ->
					{ok, null}
			end;
		false ->
			{ok, null}
	end;
execute(_, #ar_graphql_transaction { id = TXID }, <<"linkedFromTransactions">>, Args) ->
	#{ <<"byForeignTag">> := ForeignTagName, <<"tags">> := TagsOrNull } = Args,
	LinkIds = ar_tx_search:get_entries(ForeignTagName, TXID),
	case TagsOrNull of
		null ->
			SortedIds = ar_tx_search:sort_txids(LinkIds),
			{ok, [{ok, #ar_graphql_transaction { id = ar_util:encode(LinkId) }} || LinkId <- SortedIds]};
		Tags ->
			LinkSet = sets:from_list(LinkIds),
			TagMatchSet = tag_match_set(Tags),
			ResultSet = sets:intersection(LinkSet, TagMatchSet),
			ResultIds = sets:to_list(ResultSet),
			SortedIds = ar_tx_search:sort_txids(ResultIds),
			{ok, [{ok, #ar_graphql_transaction { id = ar_util:encode(Id) }} || Id <- SortedIds]}
	end;
execute(Ctx, Obj, <<"countLinkedFromTransactions">>, Args) ->
	{ok, Results} = execute(Ctx, Obj, <<"linkedFromTransactions">>, Args),
	{ok, length(Results)}.

from_txid(TXID) ->
	case ar_util:safe_decode(TXID) of
		{ok, Decoded} ->
			case ar_tx_search:get_tags_by_id(Decoded) of
				{ok, []} ->
					undefined;
				{ok, ArqlTags} ->
					Tags = lists:flatmap(fun({Name, Value}) ->
						case ar_graphql_tag:from_raw_tag(Name, Value) of
							#ar_graphql_tag {} = Tag ->
								[Tag];
							error ->
								[]
						end
					end, ArqlTags),
					#ar_graphql_transaction { id = TXID, tags = Tags }
			end;
		{error, invalid} ->
			undefined
	end.

load_tags(#ar_graphql_transaction { tags = Tags } = Tx) when is_list(Tags) ->
	Tx;
load_tags(#ar_graphql_transaction { id = TXID, tags = undefined } = Tx) ->
	{ok, ArqlTags} = ar_tx_search:get_tags_by_id(ar_util:decode(TXID)),
	Tags = lists:flatmap(fun({Name, Value}) ->
		case ar_graphql_tag:from_raw_tag(Name, Value) of
			#ar_graphql_tag {} = Tag ->
				[Tag];
			error ->
				[]
		end
	end, ArqlTags),
	Tx#ar_graphql_transaction { tags = Tags }.

tag_match_set([]) ->
	sets:from_list([]);
tag_match_set(Tags) ->
	EntrySets = lists:map(fun(#{<<"name">> := Name, <<"value">> := Value}) ->
		Entries = ar_tx_search:get_entries(Name, Value),
		sets:from_list(Entries)
	end, Tags),
	sets:intersection(EntrySets).
