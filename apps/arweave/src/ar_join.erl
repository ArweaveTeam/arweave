-module(ar_join).

-export([start/1, set_reward_history/2, set_prev_cumulative_diff/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles downloading the block index and the latest
%%% blocks from the trusted peers, to initialize the node state.

%% The number of block index elements to fetch per request.
%% Must not exceed ?MAX_BLOCK_INDEX_RANGE_SIZE defined in ar_http_iface_middleware.erl.
-ifdef(DEBUG).
-define(REQUEST_BLOCK_INDEX_RANGE_SIZE, 2).
-else.
-define(REQUEST_BLOCK_INDEX_RANGE_SIZE, 10000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start a process that will attempt to download the block index and the latest blocks.
start(Peers) ->
	spawn(fun() -> process_flag(trap_exit, true), start2(Peers) end).

%% @doc Add the corresponding reward history to every block record. We keep
%% the reward histories in the block cache and use them to validate blocks applied on top.
set_reward_history([], _RewardHistory) ->
	[];
set_reward_history(Blocks, []) ->
	Blocks;
set_reward_history([B | Blocks], RewardHistory) ->
	[B#block{ reward_history = RewardHistory } | set_reward_history(Blocks, tl(RewardHistory))].

%% @doc Add the previous cumulative difficulty to every block but the last one.
%% The previous cumulative difficulty is looked up when a potential double-signing
%% is detected.
set_prev_cumulative_diff([B, PrevB | Blocks]) ->
	[B#block{ prev_cumulative_diff = PrevB#block.cumulative_diff }
			| set_prev_cumulative_diff([PrevB | Blocks])];
set_prev_cumulative_diff([B]) ->
	[B].

%%%===================================================================
%%% Private functions.
%%%===================================================================

start2([]) ->
	?LOG_WARNING([{event, not_joining}, {reason, no_peers}]);
start2(Peers) ->
	ar:console("Joining the Arweave network...~n"),
	[{H, _, _} | _ ] = BI = get_block_index(Peers, ?REJOIN_RETRIES),
	ar:console("Downloaded the block index successfully.~n", []),
	B = get_block(Peers, H),
	do_join(Peers, B, BI).

get_block_index(Peers, Retries) ->
	case get_block_index(Peers) of
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
					timer:sleep(1000),
					erlang:halt()
			end;
		BI ->
			BI
	end.

get_block_index([]) ->
	unavailable;
get_block_index([Peer | Peers]) ->
	case get_block_index2(Peer) of
		unavailable ->
			get_block_index(Peers);
		BI ->
			BI
	end.

get_block_index2(Peer) ->
	Height = ar_http_iface_client:get_info(Peer, height),
	get_block_index2(Peer, 0, Height, []).

get_block_index2(Peer, Start, Height, BI) ->
	N = ?REQUEST_BLOCK_INDEX_RANGE_SIZE,
	case ar_http_iface_client:get_block_index(Peer, min(Start, Height),
			min(Height, Start + N - 1)) of
		{ok, Range} when length(Range) < N ->
			case Start of
				0 ->
					Range;
				_ ->
					case lists:last(Range) == hd(BI) of
						true ->
							Range ++ tl(BI);
						false ->
							unavailable
					end
			end;
		{ok, Range} when length(Range) == N ->
			case Start of
				0 ->
					get_block_index2(Peer, Start + N - 1, Height, Range);
				_ ->
					case lists:last(Range) == hd(BI) of
						true ->
							get_block_index2(Peer, Start + N - 1, Height,
									Range ++ tl(BI));
						false ->
							unavailable
					end
			end;
		_ ->
			unavailable
	end.

get_block(Peers, H) ->
	case ar_storage:read_block(H) of
		unavailable ->
			get_block(Peers, H, 10);
		BShadow ->
			Mempool = ar_node:get_pending_txs([as_map, id_only]),
			get_block(Peers, BShadow, Mempool, BShadow#block.txs, [], 10)
	end.

get_block(Peers, H, Retries) ->
	ar:console("Downloading joining block ~s.~n", [ar_util:encode(H)]),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		{_Peer, #block{} = BShadow, _Time, _Size} ->
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
					timer:sleep(1000),
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
					timer:sleep(1000),
					erlang:halt()
			end
	end.

%% @doc Perform the joining process.
do_join(Peers, B, BI) ->
	case B#block.height - ?STORE_BLOCKS_BEHIND_CURRENT > ar_fork:height_2_6() of
		true ->
			ok;
		_ ->
			ar_randomx_state:init(BI, Peers)
	end,
	ar:console("Downloading the block trail.~n", []),
	Blocks = get_block_and_trail(Peers, B, BI),
	ar:console("Downloaded the block trail successfully.~n", []),
	Blocks2 = may_be_set_reward_history(Blocks, Peers),
	Blocks3 = set_prev_cumulative_diff(Blocks2),
	ar_node_worker ! {join, BI, Blocks3},
	join_peers(Peers).

%% @doc Get a block, and its 2 * ?MAX_TX_ANCHOR_DEPTH previous blocks.
%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
%% get all existing blocks.
%%
%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
%% fork recovery (which is the deepest fork recovery possible) immediately after
%% joining the network.
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

may_be_set_reward_history([#block{ height = Height } | _] = Blocks, Peers) ->
	Fork_2_6 = ar_fork:height_2_6(),
	case Height >= Fork_2_6 of
		true ->
			Len = min(Height - Fork_2_6 + 1, ?STORE_BLOCKS_BEHIND_CURRENT),
			L = [B#block.reward_history_hash || B <- lists:sublist(Blocks, Len)],
			case ar_http_iface_client:get_reward_history(Peers, hd(Blocks), L) of
				{ok, RewardHistory} ->
					set_reward_history(Blocks, RewardHistory);
				_ ->
					ar:console("Failed to fetch the reward history for the block ~s from "
							"any of the peers. Consider changing the peers.~n",
							[ar_util:encode((hd(Blocks))#block.indep_hash)]),
					timer:sleep(1000),
					erlang:halt()
			end;
		false ->
			Blocks
	end.

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
