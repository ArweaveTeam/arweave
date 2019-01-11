-module(app_search).
-export([start/0, start/1]).
-export([start_link/1]).
-export([update_tag_table/1]).
-export([initDB/0, deleteDB/0, storeDB/3]).
-export([add_entry/3, add_entry/4, get_entries/2, get_entries/3, get_tags_by_id/3]).
-include("../ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Searchable transaction tag index.
%%%
%%% Access to the DB is wrapped in a server, which limits the number of
%%% concurrent requests to 1.
%%%
%%% In addition to the transaction tags, some auxilirary tags are added (which
%%% overwrites potential real tags with the same name).
%%%
%%% Warning! Data is never removed from the index, so it might return entries
%%% for transactions that only existed in a fork.
%%%
%%% Warning! Duplications of tags exists because every time a block is integrated
%%% into the weave, all it's transactions are written to the index no matter if
%%% the transaction was already written once before, e.g. in a different fork.
%%%
%%% Warning! The index is not complete. E.g. block_height and block_indep_hash
%%% was added at a later stage. The index includes only the transactions from
%%% the downloaded blocks.

%% @doc For compatibility. Dets database supercedes state.
-record(arql_tag, {name, value, tx}).
-record(state,{
	gossip,
	towrite = []% State of the gossip protocol.
}).

%%@doc Start a search node, linking to a supervisor process
start_link(Args) ->
	Pid = erlang:apply(app_search, start, Args),
	{ok, Pid}.

%% @doc Start a search node.
start() -> start([]).

start(Peers) ->
	initDB(),
	spawn(
		fun() ->
			server(#state{gossip = ar_gossip:init(Peers)})
		end
	).

add_entry(Name, Value, ID) -> add_entry(http_search_node, Name, Value, ID).

add_entry(ProcessName, Name, Value, ID) when not is_pid(ProcessName) ->
	add_entry(whereis(ProcessName), Name, Value, ID);
add_entry(undefined, _, _, _) -> do_nothing;
add_entry(Pid, Name, Value, ID) ->
	Pid ! {add_tx, Name, Value, ID}.

get_tags_by_id(Pid, TXID, Timeout) ->
	Pid ! {get_tags, TXID, self()},
	receive {tags, Tags} ->
		{ok, Tags}
	after Timeout ->
		{error, timeout}
	end.

%% @doc Returns a list of all transaction IDs which has the tag Name set to
%% Value. Duplicates might be returned due to the duplication in the index.
get_entries(Name, Value) -> get_entries(whereis(http_search_node), Name, Value).

get_entries(Pid, Name, Value) ->
	Pid ! {get_tx, Name, Value, self()},
	receive TXIDs ->
		TXIDs
	after 3000 ->
		[]
	end.

%% @doc Updates the table of stored tranasaction data with all of the
%% transactions in the given block
update_tag_table(B) when ?IS_BLOCK(B) ->
	lists:foreach(
		fun(TX) ->
			lists:foreach(
				fun(Tag) ->
					case Tag of
						{<<"from">>, _ } -> ok;
						{<<"to">>, _} -> ok;
						{<<"quantity">>, _} -> ok;
						{<<"reward">>, _} -> ok;
						{<<"block_height">>, _} -> ok;
						{<<"block_indep_hash">>, _} -> ok;
						{Name, Value} -> add_entry(Name, Value, TX#tx.id);
						_ -> ok
					end
				end,
				TX#tx.tags
			),
			add_entry(<<"from">>, ar_util:encode(ar_wallet:to_address(TX#tx.owner)), TX#tx.id),
			add_entry(<<"to">>, ar_util:encode(ar_wallet:to_address(TX#tx.target)), TX#tx.id),
			add_entry(<<"quantity">>, TX#tx.quantity, TX#tx.id),
			add_entry(<<"reward">>, TX#tx.reward, TX#tx.id),
			add_entry(<<"block_height">>, B#block.height, TX#tx.id),
			add_entry(<<"block_indep_hash">>, ar_util:encode(B#block.indep_hash), TX#tx.id)
		end,
		ar_storage:read_tx(B#block.txs)
	);
update_tag_table(_) ->
	not_updated.

server(S = #state { gossip = _GS }) ->
	%% Listen for gossip and normal messages.
	%% Recurse through the message box, updating one's state each time.
	try
		receive
			{get_tx, Name, Value, Pid} ->
				% ar:d({retrieving_tx, search_by_exact_tag(Name, Value)}),
				Pid ! search_by_exact_tag(Name, Value),
				server(S);
			{get_tags, TXID, Pid} ->
				Tags = lists:map(
					fun(Tag) ->
						{_, Name, Value, _} = Tag,
						{Name, Value}
					end,
					search_by_id(TXID)
				),
				Pid ! {tags, Tags},
				server(S);
			stop -> ok;
			{add_tx, Name, Value, ID} ->
				% ar:d({adding_tags, Name, Value, ID}),
				storeDB(Name, Value, ID),
				server(S);
			_OtherMsg -> server(S)
		end
	catch
		throw:Term ->
			ar:report(
				[
					{'SearchEXCEPTION', Term}
				]
			),
			server(S);
		exit:Term ->
			ar:report(
				[
					{'SearchEXIT', Term}
				]
			),
			server(S);
		error:Term ->
			ar:report(
				[
					{'SearchERROR', {Term, erlang:get_stacktrace()}}
				]
			),
			server(S)
	end.

%% @doc Initialise the mnesia database
initDB() ->
	application:set_env(mnesia, dir, "data/mnesia"),
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

%% @doc Delete the entire ARQL mnesia database
deleteDB() ->
	mnesia:delete_table(arql_tag).

%% @doc Store a transaction/tag pair in the database
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
	% Spawn a network with two nodes and a chirper server
	ar_storage:clear(),
	SearchServer = start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Generate the transaction.
	RawTX = ar_tx:new(),
	TX = RawTX#tx {tags = [{<<"TestName">>, <<"TestVal">>}]},
	% Add tx to network
	ar_node:add_tx(hd(Peers), TX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	% get TX by tag
	TXIDs = get_entries(SearchServer, <<"TestName">>, <<"TestVal">>),
	?assertEqual(true, lists:member(TX#tx.id, TXIDs)),
	% get tags by TX
	{ok, Tags} = get_tags_by_id(SearchServer, TX#tx.id, 3000),
	?assertEqual(true, lists:member({<<"TestName">>, <<"TestVal">>}, Tags)).
