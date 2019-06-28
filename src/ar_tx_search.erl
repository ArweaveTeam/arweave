-module(ar_tx_search).
-export([start/0]).
-export([update_tag_table/1]).
-export([get_entries/2, get_entries/3, get_tags_by_id/3]).
-export([delete_tx_records/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Searchable transaction tag index.
%%%
%%% Access to the DB is wrapped in a server, which limits the number of
%%% concurrent requests to 1.
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
%%%
%%% Warning! Writes returns early but are serialized together with reads. If a
%%% write fails it will be logged, but the queue of reads and writes will
%%% continue.

-record(arql_tag, {name, value, tx}).

%% @doc Start a search node.
start() ->
	initDB(),
	{ok, spawn(fun server/0)}.

delete_tx_records(TXID) ->
	delete_tx_records(whereis(http_search_node), TXID).

delete_tx_records(PID, TXID) ->
	PID ! {delete_tx_records, TXID}.

delete_for_tx(TXID) ->
	Records = mnesia:dirty_match_object(#arql_tag{tx = TXID, _ = '_'}),
	DeleteRecord = fun(Record) -> ok = mnesia:dirty_delete_object(Record) end,
	lists:foreach(DeleteRecord, Records).

get_tags_by_id(PID, TXID, Timeout) ->
	PID ! {get_tags, TXID, self()},
	receive {tags, Tags} ->
		{ok, Tags}
	after Timeout ->
		{error, timeout}
	end.

%% @doc Returns a list of all transaction IDs which has the tag Name set to
%% Value. Duplicates might be returned due to the duplication in the index.
get_entries(Name, Value) -> get_entries(whereis(http_search_node), Name, Value).

get_entries(PID, Name, Value) ->
	PID ! {get_tx, Name, Value, self()},
	receive {txs, TXIDs} ->
		TXIDs
	after 3000 ->
		[]
	end.

%% @doc Updates the index of stored tranasaction data with all of the
%% transactions in the given block. Returns early after sending the task to the
%% server.
update_tag_table(B) ->
	update_tag_table(whereis(http_search_node), B).

update_tag_table(PID, B) when ?IS_BLOCK(B) ->
	#block{
		indep_hash = IndepHash,
		height = Height,
		txs = TXs
	} = B,
	PID ! {update_tags_for_block, IndepHash, Height, TXs},
	enqueued;
update_tag_table(_, _) ->
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

server() ->
	try
		receive
			{get_tx, Name, Value, PID} ->
				% ar:d({retrieving_tx, search_by_exact_tag(Name, Value)}),
				PID ! {txs, search_by_exact_tag(Name, Value)},
				server();
			{get_tags, TXID, PID} ->
				Tags = lists:map(
					fun(Tag) ->
						{_, Name, Value, _} = Tag,
						{Name, Value}
					end,
					search_by_id(TXID)
				),
				PID ! {tags, Tags},
				server();
			{update_tags_for_block, IndepHash, Height, TXs} ->
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
				),
				server();
			{delete_tx_records, TXID} ->
				delete_for_tx(TXID),
				server();
			stop -> ok;
			_OtherMsg -> server()
		end
	catch
		throw:Term ->
			ar:report([{'SearchEXCEPTION', Term}]),
			server();
		exit:Term ->
			ar:report([{'SearchEXIT', Term}]),
			server();
		error:Term ->
			ar:report([{'SearchERROR', {Term, erlang:get_stacktrace()}}]),
			server()
	end.

%% @doc Initialise the mnesia database
initDB() ->
	TXIndexDir = filename:join(ar_meta_db:get(data_dir), ?TX_INDEX_DIR),
	%% Append the / to make filelib:ensure_dir/1 create a directory if one does not exist.
	filelib:ensure_dir(TXIndexDir ++ "/"),
	application:set_env(mnesia, dir, TXIndexDir),
	mnesia:create_schema([node()]),
	mnesia:start(),
	try
		mnesia:table_info(type, arql_tag)
	catch
		exit: _ ->
			mnesia:create_table(
				arql_tag,
				[
					{attributes, record_info(fields, arql_tag)},
					{type, bag},
					{disc_only_copies, [node()]}
				]
			)
	end.

%% @doc Store a transaction ID tag triplet in the index.
storeDB(Name, Value, TXid) ->
	mnesia:dirty_write(#arql_tag { name = Name, value = Value, tx = TXid}).

%% @doc Search for a list of transactions that match the given tag
search_by_exact_tag(Name, Value) ->
	mnesia:dirty_select(
		arql_tag,
		[
			{
				#arql_tag { name = Name, value = Value, tx = '$1'},
				[],
				['$1']
			}
		]
	).

%% @doc Search for a list of tags of the transaction with the given ID
search_by_id(TXID) ->
	mnesia:dirty_select(
		arql_tag,
		[
			{
				#arql_tag { tx = TXID, _ = '_' },
				[],
				['$_']
			}
		]
	).

basic_usage_test() ->
	ar_storage:clear(),
	{ok, SearchServer} = start(),
	[Peer | _] = ar_network:start(10, 10),
	ar_node:add_peers(Peer, SearchServer),
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
	TXIDs = get_entries(SearchServer, <<"TestName">>, <<"TestVal">>),
	?assert(lists:member(TX#tx.id, TXIDs)),
	%% Get tags by TX
	{ok, Tags} = get_tags_by_id(SearchServer, TX#tx.id, 3000),
	?assertMatch({_, <<"TestVal">>}, lists:keyfind(<<"TestName">>, 1, Tags)),
	%% Check aux tags
	?assertMatch({_, 1}, lists:keyfind(<<"block_height">>, 1, Tags)),
	%% Check that if writing the TX to the index again, the entries gets
	%% overwritten, not duplicated.
	AddTx(TX#tx{tags = [{<<"TestName">>, <<"Updated value">>}]}),
	{ok, UpdatedTags} = get_tags_by_id(SearchServer, TX#tx.id, 3000),
	?assertMatch([<<"Updated value">>], proplists:get_all_values(<<"TestName">>, UpdatedTags)).
