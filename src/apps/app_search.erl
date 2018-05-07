-module(app_search).
-export([start/0, start/1]).
-export([message/2]).
-export([start_link/1]).
-export([update_tag_table/1]).
-export([initDB/0, deleteDB/0, storeDB/3, search_by_exact_tag/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A search node for the Archain network. Given a transaction hash,
%%% returns to the requesting node the independant block hash of the
%%% block containing the transaction.
%%% For examplary purposes only.

%% txlist DETS file
-define(TXDATA, "data/txdb.dat").
%% @doc For compatibility. Dets database supercedes state.
-record(state, {
	db = [] % Stores the 'database' of links to chirps.
}).
-record(arql_tag, {name, value, tx}).


%%@doc Start a search node, linking to a supervisor process
start_link(Args) ->
	PID = erlang:apply(app_search, start, Args),
	{ok, PID}.

%% @doc Start a search node.
start() -> start([]).
start(Peers) ->
	initDB(),
	adt_simple:start(?MODULE, #state{}, Peers).

%% @doc Find the block associated with a transaction.

%% Initialise the mnesia database
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
					{disc_copies, [node()]}
				]
			)
	end.

deleteDB() ->
	mnesia:delete_table(arql_tag).

%% Store a transaction/tag pair in the database
storeDB(Name, Value, TXid) ->
	mnesia:dirty_write(#arql_tag { name = Name, value = Value, tx = TXid}).

%% Search for a list of transactions that match the given tag
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

%% @doc Add the transactions in a newly recieved block to the trasction db
%% S - current state, B - new block
%% @doc Listen for get_transaction requests, send independant block hash
%% Back to requesting process.
%% S - current state, {get_transaction - atom, T - transaction hash,
%% Process_id - id of the process to return the block hash to}
message(S, {get_tx, PID, Name, Value}) ->
	PID ! search_by_exact_tag(Name, Value),
	S;
message(S, _) ->
	S.

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
						{Name, Value} -> storeDB(Name, Value, TX#tx.id);
						_ -> ok
					end
				end,
				TX#tx.tags
			),
			storeDB(<<"from">>, ar_wallet:to_address(TX#tx.owner), TX#tx.id),
			storeDB(<<"to">>, ar_wallet:to_address(TX#tx.target), TX#tx.id),
			storeDB(<<"quantity">>, TX#tx.quantity, TX#tx.id),
			storeDB(<<"reward">>, TX#tx.reward, TX#tx.id)
		end,
		ar_storage:read_tx(B#block.txs)
	);
update_tag_table(B) ->
	not_updated.

%% @doc Add to the dets database all transactions from currently held blocks

%% @doc Test that a new tx placed on the network and mined can be searched for
basic_usage_test() ->
	% Spawn a network with two nodes and a chirper server
	ar_storage:clear(),
	SearchServer = start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Generate the transaction.
	TX = (ar_tx:new())#tx {tags = [{<<"TestName">>, <<"TestVal">>}]},
	% Add tx to network
	ar_node:add_tx(hd(Peers), TX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	% recieve a "get transaction" message
	message([], {get_tx,self(),<<"TestName">>, <<"TestVal">>}),
	% check that newly mined block matches the block the most recent transaction was mined in
	true =
		receive
			TXIDs -> lists:member(TX#tx.id, TXIDs)
		end.