-module(ar_join).

-export([
	start/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles downloading the block index and the latest
%%% blocks from the trusted peers, to initialize the node state.

%%%===================================================================
%%% Public interface.
%%%===================================================================


%% @doc Start a process that will attempt to download the block index and the latest blocks.
start(Peers) ->
	spawn(fun() -> start2(Peers) end).

%%%===================================================================
%%% Private functions.
%%%===================================================================

start2([]) ->
	?LOG_WARNING([{event, not_joining}, {reason, no_peers}]);
start2(Peers) ->
	[{H, _, _} | _ ] = BI = get_block_index(Peers, ?REJOIN_RETRIES),
	B = get_block(Peers, H),
	do_join(Peers, B, BI).

get_block_index(Peers, Retries) ->
	case ar_http_iface_client:get_block_index(Peers) of
		unavailable ->
			case Retries > 0 of
				true ->
					ar:console(
						"Failed to fetch the block index from any of the peers."
						" Retrying..~n"
					),
					?LOG_WARNING([{event, failed_to_fetch_block_index}]),
					timer:sleep(?REJOIN_TIMEOUT),
					get_block_index(Peers, Retries - 1);
				false ->
					ar:console(
						"Failed to fetch the block index from any of the peers. Giving up.."
						" Consider changing the peers.~n"
					),
					?LOG_ERROR([{event, failed_to_fetch_block_index}]),
					application:stop(arweave)
			end;
		BI ->
			BI
	end.

get_block(Peers, H) ->
	get_block(Peers, H, 10).

get_block(Peers, H, Retries) ->
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		{_, #block{} = BShadow} ->
			Mempool = ar_node:get_pending_txs([as_map, id_only]),
			get_block(Peers, BShadow, Mempool, BShadow#block.txs, [], Retries);
		_ ->
			case Retries > 0 of
				true ->
					ar:console(
						"Failed to fetch a joining block from any of the peers."
						" Retrying..~n"
					),
					?LOG_WARNING([
						{event, failed_to_fetch_joining_block},
						{block, ar_util:encode(H)}
					]),
					timer:sleep(1000),
					get_block(Peers, H, Retries - 1);
				false ->
					ar:console(
						"Failed to fetch a joining block from any of the peers. Giving up.."
						" Consider changing the peers.~n"
					),
					?LOG_ERROR([
						{event, failed_to_fetch_joining_block},
						{block, ar_util:encode(H)}
					]),
					application:stop(arweave)
			end
	end.

get_block(_Peers, BShadow, _Mempool, [], TXs, _Retries) ->
	BShadow#block{ txs = lists:reverse(TXs) };
get_block(Peers, BShadow, Mempool, [TXID | TXIDs], TXs, Retries) ->
	case ar_http_iface_client:get_tx(Peers, TXID, Mempool) of
		#tx{} = TX ->
			get_block(Peers, BShadow, Mempool, TXIDs, [TX | TXs], Retries);
		_ ->
			case Retries > 0 of
				true ->
					ar:console(
						"Failed to fetch a joining transaction from any of the peers."
						" Retrying..~n"
					),
					?LOG_WARNING([
						{event, failed_to_fetch_joining_tx},
						{tx, ar_util:encode(TXID)}
					]),
					timer:sleep(1000),
					get_block(Peers, BShadow, Mempool, [TXID | TXIDs], TXs, Retries - 1);
				false ->
					ar:console(
						"Failed to fetch a joining tx from any of the peers. Giving up.."
						" Consider changing the peers.~n"
					),
					?LOG_ERROR([
						{event, failed_to_fetch_joining_tx},
						{block, ar_util:encode(TXID)}
					]),
					application:stop(arweave)
			end
	end.

%% @doc Perform the joining process.
do_join(Peers, B, BI) ->
	ar:console("Joining the Arweave network...~n"),
	ar_arql_db:populate_db(?BI_TO_BHL(BI)),
	ar_randomx_state:init(BI, Peers),
	Blocks = get_block_and_trail(Peers, B, BI),
	ar_node_worker ! {join, BI, Blocks},
	join_peers(Peers),
	ar:console("Joined the Arweave network successfully.~n"),
	?LOG_INFO([{event, joined_the_network}]).

%% @doc Get a block, and its 2 * ?MAX_TX_ANCHOR_DEPTH previous blocks.
%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
%% get all existing blocks.
%%
%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
%% fork recovery (which is the deepest fork recovery possible) immediately after
%% joining the network.
%% @end
get_block_and_trail(Peers, NewB, BI) ->
	get_block_and_trail(Peers, NewB, 2 * ?MAX_TX_ANCHOR_DEPTH, BI).

get_block_and_trail(_Peers, NewB, BehindCurrent, _BI)
		when NewB#block.height == 0 orelse BehindCurrent == 0 ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(NewB#block.txs),
	[NewB#block{ size_tagged_txs = SizeTaggedTXs }];
get_block_and_trail(Peers, NewB, BehindCurrent, BI) ->
	PreviousBlock = get_block(Peers, NewB#block.previous_block),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(NewB#block.txs),
	[NewB#block{ size_tagged_txs = SizeTaggedTXs } |
		get_block_and_trail(Peers, PreviousBlock, BehindCurrent - 1, BI)].

join_peers(Peers) ->
	lists:foreach(
		fun(Peer) ->
			ar_http_iface_client:add_peer(Peer)
		end,
		Peers
	).

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
		ar_test_node:join_on_master(),
		ar_test_node:assert_slave_wait_until_height(2)
	end}.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	{timeout, 60, fun() ->
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
		ar_test_node:join_on_master(),
		ar_test_node:assert_slave_wait_until_height(2),
		ar_test_node:slave_mine(),
		ar_test_node:wait_until_height(3)
	end}.
