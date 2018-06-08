-module(app_search).
-export([start/0, start/1]).
-export([start_link/1]).
-export([update_tag_table/1]).
-export([initDB/0, deleteDB/0, storeDB/3]).
-export([add_entry/3, add_entry/4, get_entries/2, get_entries/3]).
-include("../ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A search node for the Archain network. Given a transaction hash,
%%% returns to the requesting node the independant block hash of the
%%% block containing the transaction.
%%% For examplary purposes only.

%% @doc For compatibility. Dets database supercedes state.
-record(arql_tag, {name, value, tx}).
-record(state,{
	gossip,
	towrite = []% State of the gossip protocol.
}).

%%@doc Start a search node, linking to a supervisor process
start_link(Args) ->
	PID = erlang:apply(app_search, start, Args),
	{ok, PID}.

%% @doc Start a search node.
start() -> start([]).
start(Peers) ->
	initDB(),
	spawn(
		fun() ->
			server(#state{gossip = ar_gossip:init(Peers)})
		end
	).

add_entry(Name, Value, ID) -> add_entry(whereis(http_search_node), Name, Value, ID).
add_entry(PID, Name, Value, ID) ->
	PID ! {add_tx, Name, Value, ID}.

get_entries(Name, Value) -> get_entries(whereis(http_search_node), Name, Value).
get_entries(PID, Name, Value) ->
	PID ! {get_tx, Name, Value, self()}.

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
						{Name, Value} -> add_entry(Name, Value, TX#tx.id);
						_ -> ok
					end
				end,
				TX#tx.tags
			),
			add_entry(<<"from">>, ar_wallet:to_address(TX#tx.owner), TX#tx.id),
			add_entry(<<"to">>, ar_wallet:to_address(TX#tx.target), TX#tx.id),
			add_entry(<<"quantity">>, TX#tx.quantity, TX#tx.id),
			add_entry(<<"reward">>, TX#tx.reward, TX#tx.id)
		end,
		ar_storage:read_tx(B#block.txs)
	);
update_tag_table(B) ->
	not_updated.

server(S = #state { gossip = _GS }) ->
	%% Listen for gossip and normal messages.
	%% Recurse through the message box, updating one's state each time.
	try
		receive
			{get_tx, Name, Value, PID} ->
				% ar:d({retrieving_tx, search_by_exact_tag(Name, Value)}),
				PID ! search_by_exact_tag(Name, Value),
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
					{'SearchEXCEPTION', {Term}}
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
	% recieve a "get transaction" message
	whereis(http_search_node) ! {get_tx,self(),<<"TestName">>, <<"TestVal">>},
	% check that newly mined block matches the block the most recent transaction was mined in
	true =
		receive
			TXIDs -> lists:member(TX#tx.id, TXIDs)
		end.
