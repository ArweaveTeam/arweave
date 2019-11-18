-module(app_queue).
-export([start/1, start/2, start/3, add/2, stop/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Starts a server that takes, signs and submits transactions to a node.
%%% The server also handles waiting until the transaction is sufficiently
%%% 'buried' in blocks, before sending the next one.
%%%
%%% Note! The transactions are never properly confirmed (by checking that they
%%% ended up in a block not on a fork), so it's possible that one transaction
%%% gets lost and the queue continues succeeding in submitting the following
%%% transactions.

-record(state, {
	node,
	wallet,
	previous_tx = none,
	previous_tx_tag_name = undefined
}).

%% How many blocks deep should be bury a TX before continuing?
-define(CONFIRMATION_DEPTH, 3).
%% How many ms should we wait between checking the current height.
-ifdef(DEBUG).
-define(POLL_INTERVAL_MS, 250).
-else.
-define(POLL_INTERVAL_MS, 15 * 1000).
-endif.

%% @doc Takes a wallet, and optionally a node (if none is supplied, the local node
%% is used).
start(Wallet) ->
	start(whereis(http_entrypoint_node), Wallet).

start(Node, Wallet) ->
	start(Node, Wallet, undefined).

start(Node, Wallet, PreviousTXTagName) ->
	spawn(
		fun() ->
			server(#state {
				node = Node,
				wallet = Wallet,
				previous_tx_tag_name = PreviousTXTagName
			})
		end
	).

%% @doc Add an unsigned TX to the queue. The server will then sign it and submit it.
add(PID, TX) ->
	PID ! {add_tx, TX}.

stop(PID) ->
	PID ! stop.

server(S) ->
	receive
		stop -> ok;
		{add_tx, TX} ->
			NewS = send_tx(S, TX),
			server(NewS)
	end.

%% @doc Send a tx to the network and wait for it to be confirmed.
send_tx(S, TX) ->
	Addr = ar_wallet:to_address(S#state.wallet),
	case ar_node:get_last_tx(S#state.node, Addr) of
		{ok, LastTXid} ->
			Price =
				ar_tx:calculate_min_tx_cost(
					byte_size(TX#tx.data),
					ar_node:get_current_diff(S#state.node),
					ar_node:get_height(S#state.node),
					ar_node:get_wallet_list(S#state.node),
					TX#tx.target
				),
			Tags = tx_tags(TX, S#state.previous_tx, S#state.previous_tx_tag_name),
			SignedTX =
				ar_tx:sign(
					TX#tx {
						last_tx = LastTXid,
						reward = Price,
						tags = Tags
					},
					S#state.wallet
				),
			ar_node:add_tx(S#state.node, SignedTX),
			ar:report(
				[
					{app, ?MODULE},
					{submitted_tx, ar_util:encode(SignedTX#tx.id)},
					{cost, SignedTX#tx.reward / ?AR(1)},
					{size, byte_size(SignedTX#tx.data)}
				]
			),
			timer:sleep(ar_node_utils:calculate_delay(byte_size(TX#tx.data))),
			StartHeight = get_current_height(S),
			wait_for_block(
				S#state { previous_tx = SignedTX#tx.id },
				StartHeight + ?CONFIRMATION_DEPTH
			);
		timeout ->
			timer:sleep(?REJOIN_TIMEOUT),
			send_tx(S, TX)
	end.

tx_tags(TX, none, _) ->
	TX#tx.tags;
tx_tags(TX, _, undefined) ->
	TX#tx.tags;
tx_tags(TX, PreviousTXID, PreviousTXTagName) ->
	TX#tx.tags ++ [{PreviousTXTagName, ar_util:encode(PreviousTXID)}].

%% @doc Wait until a given block height has been reached.
wait_for_block(S, TargetH) ->
   CurrentH = get_current_height(S),
   if CurrentH >= TargetH -> S;
   true ->
	   timer:sleep(?POLL_INTERVAL_MS),
	   wait_for_block(S, TargetH)
	end.

%% @doc Take a server state and return the current block height.
get_current_height(S) ->
	length(ar_node:get_hash_list(S#state.node)).

%%% TESTS

queue_single_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		Wallet = {_Priv1, Pub1} = ar_wallet:new(),
		Addr = crypto:strong_rand_bytes(32),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], Bs),
		Queue = start(Node1, Wallet),
		receive after 500 -> ok end,
		add(Queue, ar_tx:new(Addr, ?AR(1), ?AR(1000), <<>>)),
		receive after 500 -> ok end,
		lists:foreach(
			fun(_) ->
				ar_node:mine(Node1),
				receive after 500 -> ok end
			end,
			lists:seq(1, ?CONFIRMATION_DEPTH)
		),
		?assertEqual(?AR(1000), ar_node:get_balance(Node1, Addr))
	end}.

queue_double_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		Wallet = {_Priv1, Pub1} = ar_wallet:new(),
		Addr = crypto:strong_rand_bytes(32),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], Bs),
		Queue = start(Node1, Wallet),
		receive after 500 -> ok end,
		add(Queue, ar_tx:new(Addr, ?AR(1), ?AR(1000), <<>>)),
		add(Queue, ar_tx:new(Addr, ?AR(1), ?AR(1000), <<>>)),
		receive after 500 -> ok end,
		lists:foreach(
			fun(_) ->
				ar_node:mine(Node1),
				receive after 500 -> ok end
			end,
			lists:seq(1, ?CONFIRMATION_DEPTH * 4)
		),
		?assertEqual(?AR(2000), ar_node:get_balance(Node1, Addr))
	end}.
