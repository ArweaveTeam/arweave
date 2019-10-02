-module(app_ipfs).
-export([
	start/2, start/3,
	start_link/1, start_pinning/0,
	stop/1,
	get_and_send/2,
	bulk_get_and_send/2, bulk_get_and_send_from_file/2,
	get_block_hashes/1, get_txs/1, get_ipfs_hashes/1,
	get_local_ipfs_txs/0, add_local_ipfs_tx_data/0, add_local_ipfs_tx_data/1,
	ipfs_hash_status/1,
	maybe_ipfs_add_txs/1,
	report/1]).
-export([confirmed_transaction/2]).
-include("../ar.hrl").

-record(state,{
	adt_pid,
	wallet,
	queue,
	ipfs_name,
	ipfs_key,
	block_hashes = [],
	ipfs_hashes = [],
	txs = []
}).

%%% api

%% @doc Start pinning incoming tagged TXs to a local IPFS node.
start_pinning() ->
	Node = whereis(http_entrypoint_node),
	Wallet = undefined,
	Name = "",
	{ok, _Pid} = start([Node], Wallet, Name),
	ok.

start(Peers, Wallet) ->
	start(Peers, Wallet, "").

start(Peers, Wallet, Name) ->
	Queue = case Wallet of
		undefined -> undefined;
		_         -> app_queue:start(Wallet)
	end,
	PidMod = case Name of
		"" ->
			spawn(fun() -> server(#state{
				queue=Queue, wallet=Wallet
			}) end);
		_  ->
			{ok, Key} = make_identity(Name),
			spawn(fun() -> server(#state{
				queue=Queue, wallet=Wallet, ipfs_name=Name, ipfs_key=Key
			}) end)
		end,
	register(?MODULE, PidMod),
	PidADT = adt_simple:start(?MODULE, PidMod),
	lists:foreach(fun(Node) -> ar_node:add_peers(Node, [PidADT]) end, Peers),
	PidMod ! {add_adt_pid, PidADT},
	{ok, PidMod}.

%% @doc Start a node, linking to a supervisor process
%% function and doc comment copied from other {ar,app}_*:start_link functions.
start_link(Args) ->
	PID = erlang:apply(app_ipfs, start, Args),
	{ok, PID}.

stop(Pid) ->
	Pid ! stop,
	unregister(?MODULE).

get_and_send(Pid, Hashes) ->
	Pins = ar_ipfs:pin_ls(),
	HashesToGet = lists:filter(fun(H) -> not(lists:member(H, Pins)) end,
		Hashes),
	Q = get_x(Pid, get_queue, queue),
	lists:foreach(fun(Hash) ->
			spawn(fun() -> get_hash_and_queue(Hash, Q) end)
		end,
		HashesToGet).

bulk_get_and_send(Pid, Hashes) ->
	Pins = ar_ipfs:pin_ls(),
	HashesToGet = lists:filter(fun(H) -> not(lists:member(H, Pins)) end,
		Hashes),
	Q = get_x(Pid, get_queue, queue),
	lists:foreach(fun(Hash) -> get_hash_and_queue(Hash, Q) end, HashesToGet).

bulk_get_and_send_from_file(Pid, Filename) ->
	{ok, Hashes} = file:consult(Filename),
	bulk_get_and_send(Pid, Hashes).

get_block_hashes(Pid) ->
	get_x(Pid, get_block_hashes, block_hashes).

get_ipfs_hashes(Pid) ->
	get_x(Pid, get_ipfs_hashes, ipfs_hashes).

get_txs(Pid) ->
	get_x(Pid, get_txs, txs).

get_local_ipfs_txs() ->
	lists:map(
		fun(#{ id := ID }) -> ar_util:decode(ID) end,
		ar_sqlite3:select_txs_by([{tags, [{<<"IPFS-Add">>, any}]}])
	).

add_local_ipfs_tx_data() ->
	TXids = get_local_ipfs_txs(),
	lists:foreach(fun add_local_ipfs_tx_data/1, TXids).

add_local_ipfs_tx_data(TXid) ->
	case ar_storage:read_tx(TXid) of
		unavailable -> {error, tx_unavailable};
		TX ->
			case lists:keyfind(<<"IPFS-Add">>, 1, TX#tx.tags) of
				{<<"IPFS-Add">>, Hash} ->
					add_ipfs_data(TX, Hash);
				false ->
					{error, hash_not_found}
			end
	end.

ipfs_hash_status(Hash) ->
	Pinned = is_pinned(Hash),
	TXIDs = lists:map(
		fun(#{ id := ID }) -> ar_util:decode(ID) end,
		ar_sqlite3:select_txs_by([{tags, [{<<"IPFS-Add">>, Hash}]}])
	),
	[{hash, Hash}, {pinned, Pinned}, {tx, TXIDs}].

maybe_ipfs_add_txs(TXs) ->
	case whereis(?MODULE) of
		undefined -> not_running;
		Pid ->
			case is_process_alive(Pid) of
				false -> not_running;
				true ->
					lists:foreach(
						fun(TX) -> confirmed_transaction(Pid, TX) end,
						TXs)
			end
	end.

report(Pid) ->
	get_x(Pid, get_report, report).

%%% adt_simple callbacks
%%% return the new state (i.e. always the server pid)

confirmed_transaction(Pid, TX) ->
	Pid ! {recv_new_tx, TX},
	Pid.

%%% local server

server(State=#state{
			adt_pid=ADTPid, queue=Q, wallet=Wallet,
			ipfs_name=Name, ipfs_key=Key,
			block_hashes=BHs, ipfs_hashes=IHs, txs=TXs}) ->
	receive
		stop ->
			State#state.adt_pid ! stop,
			ok;
		{add_adt_pid, Pid} ->
			server(State#state{adt_pid=Pid});
		{get_report, From} ->
			Report = [
				{adt_pid, ADTPid},{queue, Q},{wallet, Wallet},
				{ipfs_name, Name}, {ipfs_key, Key},
				{blocks, length(BHs), safe_hd(BHs)},
				{txs, length(TXs), safe_hd(TXs)},
				{ipfs_hashes, length(IHs), safe_hd(IHs)}],
			From ! {report, Report},
			server(State);
		{get_block_hashes, From} ->
			From ! {block_hashes, BHs},
			server(State);
		{get_ipfs_hashes, From} ->
			From ! {ipfs_hashes, IHs},
			server(State);
		{get_txs, From} ->
			From ! {txs, TXs},
			server(State);
		{get_queue, From} ->
			From ! {queue, Q},
			server(State);
		{queue_tx, UnsignedTX} ->
			app_queue:add(Q, UnsignedTX),
			server(State);
		{recv_new_tx, TX=#tx{tags=Tags}} ->
			ar:d({app_ipfs, recv_new_tx, TX#tx.id}),
			case lists:keyfind(<<"IPFS-Add">>, 1, Tags) of
				{<<"IPFS-Add">>, Hash} ->
					{ok, _Hash2} = add_ipfs_data(TX, Hash),
					spawn(ar_ipfs, dht_provide_hash, [Hash]);
					%% with validation:
					%% case ar_ipfs:add_data(TX#tx.data, Hash) of
					%%	{ok, Hash} -> [Hash|IHs];
					%%	_          -> IHs
					%% end;
				false ->
					pass
			end,
			server(State);
		{recv_new_tx, X} ->
			ar:d({app_ipfs, recv_new_tx, X}),
			server(State)
	end.

%%% private functions

add_ipfs_data(TX, Hash) ->
	%% version 0.1, no validation
	ar:d({recv_tx_ipfs_add, TX#tx.id, Hash}),
	{ok, _Hash2} = ar_ipfs:add_data(TX#tx.data, Hash).

get_hash_and_queue(Hash, Queue) ->
	ar:d({fetching, Hash}),
	case ar_ipfs:cat_data_by_hash(Hash) of
		{ok, Data} ->
			{ok, Hash2} = ar_ipfs:add_data(Data, Hash),
			ar:d({added, Hash, Hash2}),
			UnsignedTX = #tx{tags=[{<<"IPFS-Add">>, Hash}], data=Data},
			app_queue:add(Queue, UnsignedTX),
			ok;
		{error, Reason} ->
			{error, Reason}
	end.

get_x(Pid, SendTag, RecvTag) ->
	Pid ! {SendTag, self()},
	receive
		{RecvTag, X} -> X
	end.

is_pinned(Hash) ->
	Pins = ar_ipfs:pin_ls(),
	lists:member(Hash, Pins).

make_identity(Name) ->
	{ok, Key} = ar_ipfs:key_gen(Name),
	ok = ar_ipfs:config_set_identity(Key),
	{ok, Key}.

safe_hd([])    -> [];
safe_hd([H|_]) -> H.
