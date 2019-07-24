-module(ar_tx_search).
-export([start/0]).
-export([update_tag_table/1]).
-export([get_entries/2, get_tags_by_id/1]).
-export([get_entries_by_tag_name/1]).
-export([get_tx_block_height/1]).
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
	{atomic, Records} = mnesia:transaction(fun() -> search_by_id(TXID) end),
	Tags = lists:map(
		fun(Record) ->
			{_, Name, Value, _} = Record,
			{Name, Value}
		end,
		Records
	),
	{ok, Tags}.

%% @doc Returns a list of all transaction IDs which have the tag Name set to
%% Value. Duplicates might be returned due to the duplication in the index.
get_entries(Name, Value) ->
	{atomic, TXIDs} = search_by_exact_tag(Name, Value),
	TXIDs.

%% @doc Returns a list of all transaction IDs which have the given tag Name.
get_entries_by_tag_name(Name) ->
	{atomic, TXIDs} = search_by_tag_name(Name),
	TXIDs.

get_tx_block_height(TXID) ->
	{atomic, [Record]} = mnesia:transaction(fun() ->
		mnesia:index_match_object(
			#arql_tag{ tx = TXID, name = <<"block_height">>, value = '_' },
			#arql_tag.tx
		)
	end),
	{Record#arql_tag.value, TXID}.

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
				case TX of
					unavailable ->
						do_nothing;
					_ ->
							delete_for_tx(TX#tx.id),
							AddEntry = fun({Name, Value}) ->
								storeDB(Name, Value, TX#tx.id)
							end,
							lists:foreach(AddEntry, entries(IndepHash, Height, TX))
				end
			end,
			ar_storage:read_tx(TXs)
		)
	end),
	ok;
update_tag_table(_) ->
	not_a_block.

entries(IndepHash, Height, TX) ->
	AuxTags = [
		{<<"from">>, ar_util:encode(ar_wallet:to_address(TX#tx.owner))},
		{<<"to">>, ar_util:encode(ar_wallet:to_address(TX#tx.target))},
		{<<"quantity">>, TX#tx.quantity},
		{<<"reward">>, TX#tx.reward},
		{<<"block_height">>, Height},
		{<<"block_indep_hash">>, ar_util:encode(IndepHash)}
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
	ok = ensure_table_exists(),
	ok = ensure_tx_index_exists().

ensure_schema_exists() ->
	Node = node(),
	case mnesia:create_schema([Node]) of
		ok -> ok;
		{error, {Node , {already_exists, Node}}} -> ok
	end.

ensure_table_exists() ->
	ok = case mnesia:create_table(arql_tag, [
		{attributes, record_info(fields, arql_tag)},
		{type, bag},
		{disc_only_copies, [node()]}
	]) of
		{atomic, ok} -> ok;
		{aborted, {already_exists, arql_tag}} -> ok
	end,
	case mnesia:wait_for_tables([arql_tag], 5000) of
		ok ->
			ok;
		{timeout, _} ->
			%% Force loading the table in case a transaction has been
			%% interrupted and machine's hostname has changed.
			%% We always work with a single Mnesia node only so it does not
			%% cause an inconsistent database state.
			yes = mnesia:force_load_table(arql_tag),
			ok
	end.

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
storeDB(Name, Value, TXid) ->
	mnesia:write(#arql_tag { name = Name, value = Value, tx = TXid}).

%% @doc Search for a list of transactions that match the given tag
search_by_exact_tag(Name, Value) ->
	mnesia:transaction(fun() ->
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
	end).

%% @doc Search for a list of transactions that match the given tag name
search_by_tag_name(Name) ->
	mnesia:transaction(fun() ->
		mnesia:select(
			arql_tag,
			[
				{
					#arql_tag { name = Name, value = '_', tx = '$1'},
					[],
					['$1']
				}
			]
		)
	end).

%% @doc Search for a list of tags for the transaction with the given ID
search_by_id(TXID) ->
	mnesia:index_match_object(#arql_tag { tx = TXID, _ = '_' }, #arql_tag.tx).

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
	%% Check that if writing the TX to the index again, the entries gets
	%% overwritten, not duplicated.
	AddTx(TX#tx{tags = [{<<"TestName">>, <<"Updated value">>}]}),
	{ok, UpdatedTags} = get_tags_by_id(TX#tx.id),
	?assertMatch([<<"Updated value">>], proplists:get_all_values(<<"TestName">>, UpdatedTags)).
