-module(ar_sim_client).
-export([start/0, start/1, start/2, start/3, start/4, stop/1]).
-export([gen_test_wallet/0]).
-export([send_random_fin_tx/0,send_random_data_tx/0, send_specified_data_tx/1]).
-export([observe/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a simulated arweave user client.
%%% Currently implemented behaviours:
%%%		- Create wallet
%%%		- Sign and add transactions to network

%% The number of peers to connect to in the gossip network.
-define(DEFAULT_NUM_CONNECTIONS, 3).
%% The maximum time to wait between actions.
%% The average case wait time will be 50% of this value.
-define(DEFAULT_ACTION_TIME, 2 * 54 * 1000).
%% Maximum length of data segment of transaction.
%% 1024 * 1024
-define(DEFAULT_MAX_TX_LEN, 1000).
%% Maximum data block size
-define(DEFAULT_MAX_DATA_LEN, 10000).
%% Location of test public/private keys
-define(WALLETLIST, "wallets/keys.csv").

-record(state, {
	key_file,
	peers,
	action_time,
	max_tx_len,
	max_data_len,
	gossip
}).

%% @doc Spawns a simulated client, when given a list
%% of peers to connect to.
start() ->
start(ar_bridge:get_remote_peers(whereis(http_bridge_node))).
start(Peers) -> start(Peers, ?DEFAULT_ACTION_TIME).
start(Peers, ActionTime) -> start(Peers, ActionTime, ?DEFAULT_MAX_TX_LEN).
start(Peers, ActionTime, MaxTXLen) ->
	start(Peers, ActionTime, MaxTXLen, ?DEFAULT_MAX_DATA_LEN).
start(Peers, ActionTime, MaxTXLen, MaxDataLen) ->
	KeyList = get_key_list(),
	spawn(
		fun() ->
			server(
				#state {
					key_file = KeyList,
					action_time = ActionTime,
					max_tx_len = MaxTXLen,
					max_data_len = MaxDataLen,
					peers = Peers
				}
			)
		end
	).

%% @doc Stop a client node.
stop(PID) ->
	PID ! stop,
	ok.

%% @doc Generate a list of allowed keys from keyfile
get_key_list() ->
	{ok, File} = file:open(?WALLETLIST, read),
	KeyList = read_key_list(File, file:read_line(File), []),
	file:close(?WALLETLIST),
	KeyList.

%% @doc Generate a genesis wallet and associated list of keys
%% write them to files
gen_test_wallet() ->
	Qty = 1000000,
	{ok, File} = file:open("genesis_wallets.csv", write),
	filelib:ensure_dir(?WALLETLIST),
	{ok, File2} = file:open(?WALLETLIST, write),
	lists:foreach(
		fun(_) ->
			{{Priv, Pub}, Pub} = ar_wallet:new_keyfile(),
			Addr = ar_wallet:to_address(Pub),
			file:write(File, [ar_util:encode(Addr) ++ "," ++ integer_to_list(Qty) ++ "\n"]),
			file:write(File2, [ar_util:encode(Priv) ++ "," ++ ar_util:encode(Pub) ++ "\n"])
		end,
		lists:seq(1,200)
	),
	file:close(File),
	file:close(File2).

%% @doc Main client server loop.
server(
	S = #state {
		key_file = KeyList,
		max_tx_len = MaxTXLen,
		max_data_len = MaxDataLen,
		action_time = ActionTime,
		peers = Peers
	}) ->
	receive
		stop -> ok
	after rand:uniform(?DEFAULT_ACTION_TIME) ->
		case rand:uniform(120) of
			120 ->
				case rand:uniform(10) of
					10 ->
						ar:d({sim_client_large_data_tx}),
						{Priv, Pub} = lists:nth(rand:uniform(50) + 950, KeyList),
						DataOpts = [
							"dummy_data/7\.5mb",
							"dummy_data/10mb"
						],
						{ok, Data} = file:read_file(
							lists:nth(rand:uniform(length(DataOpts)), DataOpts)
							),
						TX = create_data_tx({Priv, Pub}, Data);
					_ ->
						{Priv, Pub} = lists:nth(rand:uniform(50) + 950, KeyList),
						TX = create_random_data_tx({Priv, Pub}, 2000000)
				end;
			_ ->
				{Priv, Pub} = lists:nth(rand:uniform(950), KeyList),
				TX = create_random_fin_tx({Priv, Pub}, KeyList, MaxTXLen)
		end,
		ar_http_iface:send_new_tx(hd(Peers), TX),
		server(S)
	end;
server(S) ->
	ar:d(failed),
	S.

%% @doc Send a randomly created financial tx to all peers
send_random_fin_tx() ->
	KeyList = get_key_list(),
	MaxAmount = 100,
	TX = create_random_fin_tx(KeyList, MaxAmount),
	Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
	lists:foreach(
			fun(Peer) ->
				ar:report(
					[
						{sending_tx, TX#tx.id},
						{peer, Peer}
					]
				),
				ar_node:add_tx(Peer, TX)
			end,
			Peers
	).

%% @doc Send a randomly created data tx to all peers
send_random_data_tx() ->
	KeyList = get_key_list(),
	MaxTxLen = 100,
	TX = create_random_data_tx(KeyList, MaxTxLen),
	Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
	lists:foreach(
			fun(Peer) ->
				ar:report(
					[
						{sending_tx, TX#tx.id},
						{peer, Peer}
					]
				),
				ar_node:add_tx(Peer, TX)
			end,
			Peers
	).

%% @doc Send a randomly created data tx to all peers
send_specified_data_tx(Filepath) ->
	KeyList = get_key_list(),
	{ok, Data} = file:read_file(Filepath),
	TX = create_data_tx(KeyList, Data),
	Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
	lists:foreach(
			fun(Peer) ->
				ar:report(
					[
						{sending_tx, TX#tx.id},
						{peer, Peer}
					]
				),
				ar_node:add_tx(Peer, TX)
			end,
			Peers
	).

create_data_tx({Priv, Pub}, Data) ->
	LastTx = ar_node:get_last_tx(whereis(http_entrypoint_node), Pub),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	TX = ar_tx:new(Data, 0, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX)) + 550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		byte_size(<<Cost>>),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub);
create_data_tx(KeyList, Data) ->
	{Priv, Pub} = lists:nth(rand:uniform(1), KeyList),
	create_data_tx({Priv, Pub}, Data).

%% @doc Create a random data TX with max length MaxTxLen
create_random_data_tx({Priv, Pub}, MaxTxLen) ->
	% Generate and dispatch a new data transaction.
	LastTx = ar_node:get_last_tx(whereis(http_entrypoint_node), Pub),
	%ar:d({random_data_tx_pub, ar_util:encode(ar_wallet:to_address(Pub))}),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	Data = << 0:(rand:uniform(MaxTxLen) * 8) >>,
	TX = ar_tx:new(Data, 0, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX)) + 550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		byte_size(<<Cost>>),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub);

create_random_data_tx(KeyList, MaxTxLen) ->
	{Priv, Pub} = lists:nth(rand:uniform(1000), KeyList),
	% Generate and dispatch a new data transaction.
	LastTx = ar_node:get_last_tx(whereis(http_entrypoint_node), Pub),
	%ar:d({random_data_tx_pub, ar_util:encode(ar_wallet:to_address(Pub))}),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	Data = << 0:(rand:uniform(MaxTxLen) * 8) >>,
	TX = ar_tx:new(Data, 0, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX)) + 550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		byte_size(<<Cost>>),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub).

create_random_data_tx({Priv, Pub}, MaxTxLen, OldTX) ->
	% Generate and dispatch a new data transaction.
	LastTx = OldTX#tx.id,
	%ar:d({random_data_tx_pub, ar_util:encode(ar_wallet:to_address(Pub))}),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	Data = << 0:(rand:uniform(MaxTxLen) * 8) >>,
	TX = ar_tx:new(Data, 0, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX)) + 550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		byte_size(<<Cost>>),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub).
%% @doc Create a random financial TX between two wallets of amount MaxAmount
create_random_fin_tx(KeyList, MaxAmount) ->
	{Priv, Pub} = lists:nth(rand:uniform(1000), KeyList),
	{_, Dest} = lists:nth(rand:uniform(1000), KeyList),
	% Generate and dispatch a new data transaction.
	LastTx = ar_node:get_last_tx(whereis(http_entrypoint_node), Pub),
	%ar:d({random_fin_tx_pub, ar_util:encode(ar_wallet:to_address(Pub))}),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	Qty = rand:uniform(MaxAmount),
	TX = ar_tx:new(Dest, 0, Qty, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX))+550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		(byte_size(<<Cost>>)),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward, tags = [{"3123123Key", "123141515Value"}]}, Priv, Pub).

create_random_fin_tx({Priv, Pub}, KeyList, MaxAmount) ->
	{_, Dest} = lists:nth(rand:uniform(10), KeyList),
	% Generate and dispatch a new data transaction.
	LastTx = ar_node:get_last_tx(whereis(http_entrypoint_node), Pub),
	%ar:d({random_fin_tx_pub, ar_util:encode(ar_wallet:to_address(Pub))}),
	Diff = ar_node:get_diff(whereis(http_entrypoint_node)),
	Qty = rand:uniform(MaxAmount),
	TX = ar_tx:new(Dest, 0, Qty, LastTx),
	Cost = ar_tx:calculate_min_tx_cost(
		byte_size(ar_tx:tx_to_binary(TX))+550,
		Diff
		),
	Reward = Cost + ar_tx:calculate_min_tx_cost(
		(byte_size(<<Cost>>)),
		Diff
		),
	ar_tx:sign(TX#tx{reward = Reward}, Priv, Pub).
%% @doc Read a list of public/private keys from a file
read_key_list(_File, eof, Keys) ->
	Keys;
read_key_list(File, {ok, Line}, Keys) ->
	Array = string:split(Line, ","),
	Priv = ar_util:decode(lists:nth(1, Array)),
	Pub = ar_util:decode(string:trim(lists:nth(2, Array), trailing, "\n")),
	read_key_list(File, file:read_line(File), [{{Priv, Pub}, Pub}|Keys]).

%% @doc a simulation of the shadowplay system
observe() ->
	observer:start().