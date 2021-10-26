-module(ar_join).

-export([
	start/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
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
	ar:console("Joining the Arweave network...~n"),
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
					erlang:halt()
			end;
		BI ->
			BI
	end.

get_block(Peers, H) ->
	get_block(Peers, H, 10).

get_block(Peers, H, Retries) ->
	ar:console("Downloading joining block ~s.~n", [ar_util:encode(H)]),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		{_, #block{} = BShadow} ->
			Mempool = ar_node:get_pending_txs([as_map, id_only]),
			get_block(Peers, BShadow, Mempool, BShadow#block.txs, [], Retries);
		_ ->
			case Retries > 0 of
				true ->
					ar:console(
						"Failed to fetch a joining block ~s from any of the peers."
						" Retrying..~n", [ar_util:encode(H)]
					),
					?LOG_WARNING([
						{event, failed_to_fetch_joining_block},
						{block, ar_util:encode(H)}
					]),
					timer:sleep(1000),
					get_block(Peers, H, Retries - 1);
				false ->
					ar:console(
						"Failed to fetch a joining block ~s from any of the peers. Giving up.."
						" Consider changing the peers.~n", [ar_util:encode(H)]
					),
					?LOG_ERROR([
						{event, failed_to_fetch_joining_block},
						{block, ar_util:encode(H)}
					]),
					erlang:halt()
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
						"Failed to fetch a joining transaction ~s from any of the peers."
						" Retrying..~n", [ar_util:encode(TXID)]
					),
					?LOG_WARNING([
						{event, failed_to_fetch_joining_tx},
						{tx, ar_util:encode(TXID)}
					]),
					timer:sleep(1000),
					get_block(Peers, BShadow, Mempool, [TXID | TXIDs], TXs, Retries - 1);
				false ->
					ar:console(
						"Failed to fetch a joining tx ~s from any of the peers. Giving up.."
						" Consider changing the peers.~n", [ar_util:encode(TXID)]
					),
					?LOG_ERROR([
						{event, failed_to_fetch_joining_tx},
						{block, ar_util:encode(TXID)}
					]),
					erlang:halt()
			end
	end.

%% @doc Perform the joining process.
do_join(Peers, B, BI) ->
	ar_randomx_state:init(BI, Peers),
	ar:console("Downloading the block trail.~n", []),
	Blocks = get_block_and_trail(Peers, B, BI),
	ar:console("Downloaded the block trail successfully.~n", []),
	ar_node_worker ! {join, BI, Blocks},
	join_peers(Peers).

%% @doc Get a block, and its 2 * ?MAX_TX_ANCHOR_DEPTH previous blocks.
%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
%% get all existing blocks.
%%
%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
%% fork recovery (which is the deepest fork recovery possible) immediately after
%% joining the network.
%% @end
get_block_and_trail(Peers, B, BI) ->
	Trail = lists:sublist(tl(BI), 2 * ?MAX_TX_ANCHOR_DEPTH),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs, B#block.height),
	[B#block{ size_tagged_txs = SizeTaggedTXs } | get_block_and_trail(Peers, Trail)].

get_block_and_trail(Peers, Trail) when length(Trail) < 10 ->
	ar_util:pmap(fun({H, _, _}) -> get_block_and_trail(Peers, H) end, Trail);
get_block_and_trail(Peers, Trail) when is_list(Trail) ->
	{Chunk, Trail2} = lists:split(10, Trail),
	ar_util:pmap(fun({H, _, _}) -> get_block_and_trail(Peers, H) end, Chunk)
			++ get_block_and_trail(Peers, Trail2);

get_block_and_trail(Peers, H) ->
	B = get_block(Peers, H),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs, B#block.height),
	B#block{ size_tagged_txs = SizeTaggedTXs }.

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
