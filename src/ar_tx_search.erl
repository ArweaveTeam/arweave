-module(ar_tx_search).
-export([start/0]).
-export([update_tag_table/1]).
-export([get_entries/2, get_tags_by_id/1]).
-export([get_entries_by_tag_name/1]).
-export([sort_txids/1]).
-export([delete_tx_records/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Searchable transaction tag index.
%%%
%%% In addition to the transaction tags, some auxilirary tags are added (which
%%% overwrites potential real tags with the same name).
%%%
%%% Warning! Data is not properly removed from the index. E.g. if doing a fork
%%% recovery to a fork where a transaction doesn't exist anymore, then the
%%% transactions is never removed from the index. If the transaction does exist
%%% also in the fork, then the transaction in the index is updated.
%%%
%%% Warning! The index is not complete. E.g. block_height and block_indep_hash
%%% was added at a later stage. The index includes only the transactions from
%%% the downloaded blocks.

-record(arql_tag, {name, value, tx}).
-record(arql_block, {tx, block_indep_hash, block_height}).

%% @doc Start a search node.
start() ->
	initDB(),
	ok.

delete_tx_records(TXID) ->
	{atomic, ok} = mnesia:transaction(fun() ->
		delete_for_tx(TXID)
	end),
	ok.

delete_for_tx(TXID) ->
	Records = mnesia:index_match_object(#arql_tag{tx = TXID, _ = '_'}, #arql_tag.tx),
	lists:foreach(
		fun(Record) -> ok = mnesia:delete_object(Record) end,
		Records
	).

get_tags_by_id(TXID) ->
	{atomic, Records} = mnesia:transaction(
		fun() ->
			Tags = lists:map(
				fun(Record) ->
					{_, Name, Value, _} = Record,
					{Name, Value}
				end,
				search_arql_tag_by_id(TXID)
			),
			BlockInfo = search_arql_block_by_id(TXID),
			case BlockInfo of
				[] ->
					%% For the old records block information is stored along with the tags.
					Tags;
				[{IndepHash, Height}] ->
					%% The old records contain the block information which needs to be
					%% replaced by the relevant information from the new table.
					Tags2 = lists:keystore(<<"block_indep_hash">>, 1, Tags, {<<"block_indep_hash">>, IndepHash}),
					lists:keystore(<<"block_height">>, 1, Tags2, {<<"block_height">>, Height})
			end
		end
	),
	{ok, Records}.

%% @doc Returns a list of all transaction IDs which have the given tag Name.
get_entries_by_tag_name(Name) ->
	{atomic, TXIDs} = search_by_tag_name(Name),
	TXIDs.

sort_txids(TXIDs) ->
	{atomic, SearchList} = mnesia:transaction(fun() ->
		NewEntries = mnesia:select(arql_block, [{
			#arql_block { block_height = '$2', tx = '$1', block_indep_hash = '_' },
			[],
			[{{'$1', '$2'}}]
		}]),
		OldEntries = mnesia:select(arql_tag, [{
			#arql_tag { name = <<"block_height">>, tx = '$1', value = '$2'},
			[],
			[{{'$1', '$2'}}]
		}]),
		ar_util:unique(OldEntries ++ NewEntries)
	end),
	{_, TXIDHeightPairs} = lists:foldl(
		fun(TXID, {CurrentSearchList, TXIDHeightPairs}) ->
			case lists:keytake(TXID, 1, CurrentSearchList) of
				{value, {TXID, Height}, NewSearchList} ->
					{NewSearchList, [{Height, TXID} | TXIDHeightPairs]};
				false ->
					{CurrentSearchList, TXIDHeightPairs}
			end
		end,
		{SearchList, []},
		TXIDs
	),
	SortedPairs = lists:sort(fun(A, B) -> A >= B end, TXIDHeightPairs),
	SortedTXIDs = lists:map(fun({_, TXID}) -> TXID end, SortedPairs),
	SortedTXIDs.

%% @doc Updates the tag index with the information of all the transactions in
%% the given block as part of a single transaction.
update_tag_table(B) when ?IS_BLOCK(B) ->
	#block{
		indep_hash = IndepHash,
		height = Height,
		txs = TXs
	} = B,
	{atomic, ok} = mnesia:transaction(fun() ->
		lists:foreach(
			fun(TX) ->
				AddEntry = fun({Name, Value}) ->
					storeTags(Name, Value, TX#tx.id)
				end,
				lists:foreach(AddEntry, entries(TX)),
				storeBlockInfo(TX#tx.id, ar_util:encode(IndepHash), Height)
			end,
			TXs
		)
	end),
	ok;
update_tag_table(_) ->
	not_a_block.

entries(TX) ->
	AuxTags = [
		{<<"from">>, ar_util:encode(ar_wallet:to_address(TX#tx.owner))},
		{<<"to">>, ar_util:encode(ar_wallet:to_address(TX#tx.target))},
		{<<"quantity">>, TX#tx.quantity},
		{<<"reward">>, TX#tx.reward}
	],
	AuxTags ++ multi_delete(TX#tx.tags, proplists:get_keys(AuxTags)).

multi_delete(Proplist, []) ->
	Proplist;
multi_delete(Proplist, [Key | Keys]) ->
	multi_delete(proplists:delete(Key, Proplist), Keys).

%% @doc Initialise the mnesia database
initDB() ->
	TXIndexDir = filename:join(ar_meta_db:get(data_dir), ?TX_INDEX_DIR),
	%% Append the / to make filelib:ensure_dir/1 create a directory if one does not exist.
	ok = filelib:ensure_dir(TXIndexDir ++ "/"),
	ok = application:set_env(mnesia, dir, TXIndexDir),
	ok = ensure_schema_exists(),
	ok = mnesia:start(),
	ok = ensure_arql_tag_exists(),
	ok = ensure_arql_block_exists(),
	ok = ensure_tx_index_exists().

ensure_schema_exists() ->
	Node = node(),
	case mnesia:create_schema([Node]) of
		ok -> ok;
		{error, {Node , {already_exists, Node}}} -> ok
	end.

ensure_arql_tag_exists() ->
	ok = case mnesia:create_table(arql_tag, [
		{attributes, record_info(fields, arql_tag)},
		{type, bag},
		{disc_only_copies, [node()]}
	]) of
		{atomic, ok} -> ok;
		{aborted, {already_exists, arql_tag}} -> ok
	end,
	ok = mnesia:wait_for_tables([arql_tag], 60000),
	ok.

ensure_arql_block_exists() ->
	ok = case mnesia:create_table(arql_block, [
		{attributes, record_info(fields, arql_block)},
		{type, set},
		{disc_only_copies, [node()]}
	]) of
		{atomic, ok} -> ok;
		{aborted, {already_exists, arql_block}} -> ok
	end,
	ok = mnesia:wait_for_tables([arql_block], 60000),
	ok.

ensure_tx_index_exists() ->
	{Time, Value} = timer:tc(fun() -> mnesia:add_table_index(arql_tag, #arql_tag.tx) end),
	case Value of
		{atomic, ok} ->
			ar:info([ar_tx_search, added_tx_index, {microseconds, Time}]),
			ok;
		{aborted, {already_exists, arql_tag, #arql_tag.tx}} ->
			ok
	end.

%% @doc Store a transaction ID tag triplet in the index.
storeTags(Name, Value, TXID) ->
	mnesia:write(#arql_tag { name = Name, value = Value, tx = TXID}).

%% @doc Store a transaction ID, block independent hash, block height triplet in the index.
storeBlockInfo(TXID, BlockIndepHash, BlockHeight) ->
	mnesia:write(#arql_block { tx = TXID, block_indep_hash = BlockIndepHash, block_height = BlockHeight }).

%% @doc Returns a list of all transaction IDs which have the tag Name set to
%% Value. Duplicates might be returned due to the duplication in the index.
get_entries(Name, Value) ->
	%% Fetch the block height and the block hash from the new table
	%% if it is there, otherwise fall back to the arql_tag table.
	ShouldSearchInARQLTagTable = case Name of
		<<"block_height">> ->
			case search_arql_block_by_height(Value) of
				[] ->
					true;
				Result ->
					{false, Result}
			end;
		<<"block_indep_hash">> ->
			case search_arql_block_by_indep_hash(Value) of
				[] ->
					true;
				Result ->
					{false, Result}
			end;
		_ ->
			true
	end,
	case ShouldSearchInARQLTagTable of
		{false, TXIDs} ->
			TXIDs;
		true ->
			{atomic, TXIDs} = mnesia:transaction(fun() ->
				mnesia:select(
					arql_tag,
					[
						{
							#arql_tag { name = Name, value = Value, tx = '$1'},
							[],
							['$1']
						}
					]
				)
			end),
			TXIDs
	end.

search_arql_block_by_indep_hash(IndepHash) ->
	{atomic, TXIDs} = mnesia:transaction(fun() ->
		mnesia:select(
			arql_block,
			[
				{
					#arql_block { block_height = '_', block_indep_hash = IndepHash, tx = '$1'},
					[],
					['$1']
				}
			]
		)
	end),
	TXIDs.

search_arql_block_by_height(Height) ->
	{atomic, TXIDs} = mnesia:transaction(fun() ->
		mnesia:select(
			arql_block,
			[
				{
					#arql_block { block_height = Height, block_indep_hash = '_', tx = '$1'},
					[],
					['$1']
				}
			]
		)
	end),
	TXIDs.

%% @doc Search for a list of transactions that match the given tag name
search_by_tag_name(Name) ->
	mnesia:transaction(
		fun() ->
			BlockTableTXIDs = case Name of
				<<"block_height">> ->
					search_arql_block_by_height('_');
				<<"block_indep_hash">> ->
					search_arql_block_by_indep_hash('_');
				_ ->
					[]
			end,
			TagTableTXIDs = mnesia:select(
				arql_tag,
					[
						{
							#arql_tag { name = Name, value = '_', tx = '$1'},
							[],
							['$1']
						}
					]
				),
			ar_util:unique(BlockTableTXIDs ++ TagTableTXIDs)
		end
	).

%% @doc Search for a list of tags for the transaction with the given ID
search_arql_tag_by_id(TXID) ->
	mnesia:index_match_object(#arql_tag { tx = TXID, _ = '_' }, #arql_tag.tx).

search_arql_block_by_id(TXID) ->
	mnesia:select(
		arql_block,
		[
			{
				#arql_block { tx = TXID, block_indep_hash = '$1', block_height = '$2' },
				[],
				[{{'$1', '$2'}}]
			}
		]
	).

basic_usage_test() ->
	ar_storage:clear(),
	ok = start(),
	[Peer | _] = ar_network:start(10, 10),
	% Generate the transaction.
	RawTX = ar_tx:new(),
	TX = RawTX#tx {tags = [
		{<<"TestName">>, <<"TestVal">>},
		{<<"block_height">>, <<"user-specified-block-height">>}
	]},
	AddTx = fun(TXToAdd) ->
		%% Add tx to network
		ar_node:add_tx(Peer, TXToAdd),
		%% Begin mining
		receive after 250 -> ok end,
		ar_node:mine(Peer),
		receive after 1000 -> ok end
	end,
	AddTx(TX),
	%% Get TX by tag
	TXIDs = get_entries(<<"TestName">>, <<"TestVal">>),
	?assert(lists:member(TX#tx.id, TXIDs)),
	%% Get tags by TX
	{ok, Tags} = get_tags_by_id(TX#tx.id),
	?assertMatch({_, <<"TestVal">>}, lists:keyfind(<<"TestName">>, 1, Tags)),
	%% Check aux tags
	?assertMatch({_, 1}, lists:keyfind(<<"block_height">>, 1, Tags)),
	%% Write a new block height and a new block hash to the TX index. Expect the
	%% record to be overwritten.
	AddTx(TX),
	{ok, UpdatedTags} = get_tags_by_id(TX#tx.id),
	?assertMatch({_, 2}, lists:keyfind(<<"block_height">>, 1, UpdatedTags)).
