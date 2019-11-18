-module(app_autoupdate).
-export([start/1, new_transaction/2, uniform_wait/2]).
-export([update_tx_test_dangerous/0]).
-export([nonupdate_tx_test_dangerous/0]).
-export([mixed_tx_test_dangerous/0]).
-export([multiple_update_tx_test_dangerous/0]).
-include("../ar.hrl").

%%% An application that watches a given address for incoming transactions.
%%% Upon receiving an inbound tx for the given addresses, updates the node
%%% from the latest changes on the repo.

%% @doc Start the autoupdater.
%% NB: The address set relates to the wallet in which will cause the client
%% to update when posting a tx.
start(Address) ->
	ar:report_console(
		[
			started_auto_updater,
			{watch_address, ar_util:encode(Address)}
		]
	),
	adt_simple:start(?MODULE, Address).

%% @doc New block callback function. Checks whether the block contains
%% any txs relevant to set autoupdate wallet.
new_transaction(Addr, T) ->
	case ar_wallet:to_address(T#tx.owner) of
		Addr ->
			process_update(T);
		_ -> do_nothing
	end,
	Addr.

%% @doc Handle receiving an update TX.
process_update(TX) ->
	io:format(
		"~n"
		"====================~n"
		"AUTO-UPDATE Transaction received:~n"
		"~s~n"
		"====================~n",
		[TX#tx.data]
	),
	update(),
	% Wait a random amount of time before the restarting the updated node.
	Wait = uniform_wait(2, hours),
	receive after Wait -> ok end,
	ar:report([restarting_node, {reason, auto_update}]),
	% End with error code 1 to allow heartbeat to restart the server
	erlang:halt(1).

%% @doc Pull the latest changes from the Arweave git repository.
update() ->
	Res = os:cmd("git fetch --tags; git -c advice.detachedHead=false checkout stable"),
	ar:report([auto_update_finished, {response, Res}]),
	io:format(
		"Executed update. Result: ~n~s~n"
		"====================~n",
		[Res]
	).

%% @doc Returns a uniform wait time in milliseconds between 0 and the max
%% value given to wait.
uniform_wait(Hours, hours) ->
	rand:uniform(Hours * 60 * 60) * 1000;
uniform_wait(Minutes, minutes) ->
	rand:uniform(Minutes * 60) * 1000;
uniform_wait(Seconds, seconds) ->
	rand:uniform(Seconds) * 1000.

%%% Tests: app_autoupdate.

%% @doc Ensure that an update tx causes the client to update.
%% NB: Cannot be run in the test suite as it kills the active erlang instance.
update_tx_test_dangerous() ->
	{UpdatePriv, UpdatePub} = ar_wallet:new(),
	B0 = ar_weave:init([{ar_wallet:to_address(UpdatePub), ?AR(1000), <<>>}]),
	Node = ar_node:start([], B0),
	Updater = start(ar_wallet:to_address(UpdatePub)),
	ar_node:add_peers(Node, Updater),
	UpdateTX = ar_tx:new(<<"- Testing update.">>, ?AR(1), <<>>),
	SignedTX = ar_tx:sign(UpdateTX, UpdatePriv, UpdatePub),
	ar_node:add_tx(Node, SignedTX),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	ok.

%% @doc Ensure that a non-update tx does not cause the client ot update.
nonupdate_tx_test_dangerous() ->
	{_, UpdatePub} = ar_wallet:new(),
	{Priv, Pub} = ar_wallet:new(),
	B0 = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(1000), <<>>}]),
	Node = ar_node:start([], B0),
	Updater = start(ar_wallet:to_address(UpdatePub)),
	ar_node:add_peers(Node, Updater),
	NormalTX = ar_tx:new(<<"- Testing update.">>, ?AR(1), <<>>),
	SignedTX = ar_tx:sign(NormalTX, Priv, Pub),
	ar_node:add_tx(Node, SignedTX),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	ok.

%% @doc Ensure that a block containing both an update tx and non-update tx
%% causes the client to update.
%% NB: Cannot be run in the test suite as it kills the active erlang instance.
mixed_tx_test_dangerous() ->
	{UpdatePriv, UpdatePub} = ar_wallet:new(),
	{Priv, Pub} = ar_wallet:new(),
	B0 =
		ar_weave:init(
			[
				{ar_wallet:to_address(Pub), ?AR(1000), <<>>},
				{ar_wallet:to_address(UpdatePub), ?AR(500), <<>>}
			]
		),
	Node = ar_node:start([], B0),
	Updater = start(ar_wallet:to_address(UpdatePub)),
	ar_node:add_peers(Node, Updater),
	TX = ar_tx:new(<<"Normal tx.">>, ?AR(1), <<>>),
	SignedTX = ar_tx:sign(TX, Priv, Pub),
	UpdateTX = ar_tx:new(<<"- Testing update.">>, ?AR(1), <<>>),
	SignedUpdateTX = ar_tx:sign(UpdateTX, UpdatePriv, UpdatePub),
	ar_node:add_tx(Node, SignedUpdateTX),
	receive after 500 -> ok end,
	ar_node:add_tx(Node, SignedTX),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	ok.

%% @doc Ensure that posting multiple update txs to the same block does not
%% cause erroneous behaviour.
%% NB: A non-issue as a single wallet can only have one tx per block.
%% NB: Cannot be run in the test suite as it kills the active erlang instance.
multiple_update_tx_test_dangerous() ->
	{UpdatePriv, UpdatePub} = ar_wallet:new(),
	B0 = ar_weave:init([{ar_wallet:to_address(UpdatePub), ?AR(1000), <<>>}]),
	Node = ar_node:start([], B0),
	Updater = start(ar_wallet:to_address(UpdatePub)),
	ar_node:add_peers(Node, Updater),
	UpdateTX1 = ar_tx:new(<<"- Testing update 1.">>, ?AR(1), <<>>),
	SignedUpdateTX1 = ar_tx:sign(UpdateTX1, UpdatePriv, UpdatePub),
	UpdateTX2 = ar_tx:new(<<"- Testing update 2.">>, ?AR(1), <<>>),
	SignedUpdateTX2 = ar_tx:sign(UpdateTX2, UpdatePriv, UpdatePub),
	ar_node:add_tx(Node, SignedUpdateTX1),
	ar_node:add_tx(Node, SignedUpdateTX2),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	ok.