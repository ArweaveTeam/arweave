-module(app_search).
-export([start/0, start/1]).
-export([new_block/2, message/2]).
-export([build_list/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A search node for the Archain network. Given a transaction hash,
%%% returns to the requesting node the independant block hash of the
%%% block containing the transaction.
%%% For examplary purposes only.

%% For compatibility. Dets database supercedes state.
-record(state, {
	db = [] % Stores the 'database' of links to chirps.
}).


%% Start a search node.
start() -> start([]).
start(Peers) ->
	filelib:ensure_dir("src/apps/tx/"),
	adt_simple:start(?MODULE, #state{}, Peers).
	

%% Add the transactions in a newly recieved block to the trasction db
%% S - current state, B - new block
new_block(S,B) ->
	{ok,Ref} = dets:open_file("src/apps/tx/txlist.file",[]),
	{ok,Ref} = insert_transactions(Ref, B#block.txs, B#block.indep_hash),
	dets:close(Ref),
	S.

%% Listen for get_transaction requests, send independant block hash
%% Back to requesting process.
%% S - current state, {get_transaction - atom, T - transaction hash,
%% Process_id - id of the process to return the block hash to}
message(S,{get_transaction,T,Process_id}) ->
	{ok,Ref} = dets:open_file("src/apps/tx/txlist.file",[]),
	Out = dets:lookup(Ref, T),
	dets:close(Ref),
	Process_id ! Out,
	S;
message(S, _) ->
	S.

%% Add to the dets database all transactions from currently held blocks
build_list() ->
	{ok, Filenames} = file:list_dir("blocks"),
	lists:foreach(fun(Filename) -> 
						{ok, Binary} = file:read_file("blocks/" ++ Filename),
						Block = ar_serialize:json_struct_to_block(binary_to_list(Binary)),
						new_block(new, Block)
					end,
					Filenames
				).

%% Insert transactions into the dets database
%% Ref - Ref to dets DB, [T - Transaction hash, Txlist - List of transactions],
%% Bhash - Independant block hash
insert_transactions(Ref, [], _) -> 
	{ok, Ref};
insert_transactions(Ref, [T|Txlist], Bhash) ->
	io:format("Txs ~p~n",[Txlist]),
	dets:insert(Ref, {T#tx.id,Bhash}),
	insert_transactions(Ref, Txlist, Bhash).

