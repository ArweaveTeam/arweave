-module(ar_join).

-export([start/1, set_reward_history/2, set_block_time_history/2]).

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
	spawn(fun() -> process_flag(trap_exit, true), start2(filter_peers(Peers)) end).

%% @doc Add the corresponding reward history to every block record. We keep
%% the reward histories in the block cache and use them to validate blocks applied on top.
set_reward_history([], _RewardHistory) ->
	[];
set_reward_history(Blocks, []) ->
	Blocks;
set_reward_history([B | Blocks], RewardHistory) ->
	[B#block{ reward_history = RewardHistory } | set_reward_history(Blocks, tl(RewardHistory))].

set_block_time_history([], _BlockTimeHistory) ->
	[];
set_block_time_history(Blocks, []) ->
	Blocks;
set_block_time_history([B | Blocks], BlockTimeHistory) ->
	[B#block{ block_time_history = BlockTimeHistory }
			| set_block_time_history(Blocks, tl(BlockTimeHistory))].

%%%===================================================================
%%% Private functions.
%%%===================================================================

filter_peers(Peers) ->
	filter_peers(Peers, []).

filter_peers([Peer | Peers], Peers2) ->
	case ar_http_iface_client:get_info(Peer, height) of
		info_unavailable ->
			?LOG_WARNING([{event, trusted_peer_unavailable},
					{peer, ar_util:format_peer(Peer)}]),
			filter_peers(Peers, Peers2);
		Height ->
			filter_peers(Peers, [{Height, Peer} | Peers2])
	end;
filter_peers([], []) ->
	[];
filter_peers([], Peers2) ->
	MaxHeight = lists:max([Height || {Height, _Peer} <- Peers2]),
	filter_peers2(Peers2, MaxHeight).

filter_peers2([], _MaxHeight) ->
	[];
filter_peers2([{Height, Peer} | Peers], MaxHeight) when MaxHeight - Height >= 5 ->
	?LOG_WARNING([{event, trusted_peer_five_or_more_blocks_behind},
			{peer, ar_util:format_peer(Peer)}]),
	filter_peers2(Peers, MaxHeight);
filter_peers2([{_Height, Peer} | Peers], MaxHeight) ->
	[Peer | filter_peers2(Peers, MaxHeight)].

start2([]) ->
	ar:console("~nTrusted peers are not available.~n", []),
	?LOG_WARNING([{event, not_joining}, {reason, trusted_peers_not_available}]),
	timer:sleep(1000),
	erlang:halt();
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
			get_block(Peers, BShadow, BShadow#block.txs, [], 10)
	end.

get_block(Peers, H, Retries) ->
	ar:console("Downloading joining block ~s.~n", [ar_util:encode(H)]),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		{_Peer, #block{} = BShadow, _Time, _Size} ->
			get_block(Peers, BShadow, BShadow#block.txs, [], Retries);
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

get_block(_Peers, BShadow, [], TXs, _Retries) ->
	BShadow#block{ txs = lists:reverse(TXs) };
get_block(Peers, BShadow, [TXID | TXIDs], TXs, Retries) ->
	case ar_http_iface_client:get_tx(Peers, TXID) of
		#tx{} = TX ->
			get_block(Peers, BShadow, TXIDs, [TX | TXs], Retries);
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
					get_block(Peers, BShadow, [TXID | TXIDs], TXs, Retries - 1);
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
	ar:console("Downloading the block trail.~n", []),
	{ok, Config} = application:get_env(arweave, config),
	WorkerQ = queue:from_list([spawn(fun() -> worker() end)
			|| _ <- lists:seq(1, Config#config.join_workers)]),
	PeerQ = queue:from_list(Peers),
	Trail = lists:sublist(tl(BI), 2 * ?MAX_TX_ANCHOR_DEPTH),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(B#block.txs, B#block.height),
	Retries = lists:foldl(fun(Peer, Acc) -> maps:put(Peer, 10, Acc) end, #{}, Peers),
	Blocks = [B#block{ size_tagged_txs = SizeTaggedTXs }
			| get_block_trail(WorkerQ, PeerQ, Trail, Retries)],
	ar:console("Downloaded the block trail successfully.~n", []),
	Blocks2 = may_be_set_reward_history(Blocks, Peers),
	Blocks3 = may_be_set_block_time_history(Blocks2, Peers),
	ar_node_worker ! {join, B#block.height, BI, Blocks3},
	join_peers(Peers).

%% @doc Get the 2 * ?MAX_TX_ANCHOR_DEPTH blocks preceding the head block.
%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
%% get all existing blocks.
%%
%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
%% fork recovery (which is the deepest fork recovery possible) immediately after
%% joining the network.
get_block_trail(_WorkerQ, _PeerQ, [], _Retries) ->
	[];
get_block_trail(WorkerQ, PeerQ, Trail, Retries) ->
	{WorkerQ2, PeerQ2} = request_blocks(Trail, WorkerQ, PeerQ),
	FetchState = #{ awaiting_block_count => length(Trail) },
	get_block_trail_loop(WorkerQ2, PeerQ2, Retries, Trail, FetchState).

request_blocks([], WorkerQ, PeerQ) ->
	{WorkerQ, PeerQ};
request_blocks([{H, _, _} | Trail], WorkerQ, PeerQ) ->
	{{value, W}, WorkerQ2} = queue:out(WorkerQ),
	{{value, Peer}, PeerQ2} = queue:out(PeerQ),
	W ! {get_block_shadow, H, Peer, self()},
	request_blocks(Trail, queue:in(W, WorkerQ2), queue:in(Peer, PeerQ2)).

get_block_trail_loop(WorkerQ, PeerQ, Retries, Trail, FetchState) ->
	receive
		{block_response, H, _Peer, {_, #block{} = BShadow, _, _}} ->
			ar_disk_cache:write_block_shadow(BShadow),
			TXCount = length(BShadow#block.txs),
			FetchState2 = maps:put(H, {BShadow, #{}, TXCount}, FetchState),
			AwaitingBlockCount = maps:get(awaiting_block_count, FetchState2),
			AwaitingBlockCount2 =
				case TXCount of
					0 ->
						?LOG_INFO([{event, join_remaining_blocks_to_fetch},
							{remaining_blocks_count, AwaitingBlockCount - 1}]),
						AwaitingBlockCount - 1;
					_ ->
						AwaitingBlockCount
				end,
			FetchState3 = maps:put(awaiting_block_count, AwaitingBlockCount2, FetchState2),
			{WorkerQ2, PeerQ2} = request_txs(H, BShadow#block.txs, WorkerQ, PeerQ),
			case AwaitingBlockCount2 of
				0 ->
					get_blocks(Trail, FetchState3);
				_ ->
					get_block_trail_loop(WorkerQ2, PeerQ2, Retries, Trail, FetchState3)
			end;
		{block_response, H, Peer, Response} ->
			PeerRetries = maps:get(Peer, Retries),
			case PeerRetries > 0 of
				true ->
					ar:console("Failed to fetch a joining block ~s from ~s."
							" Retrying..~n", [ar_util:encode(H), ar_util:format_peer(Peer)]),
					?LOG_WARNING([
						{event, failed_to_fetch_joining_block},
						{block, ar_util:encode(H)},
						{peer, ar_util:format_peer(Peer)},
						{response, io_lib:format("~p", [Response])}
					]),
					timer:sleep(1000),
					Retries2 = maps:put(Peer, PeerRetries - 1, Retries),
					{WorkerQ2, PeerQ2} = request_block(H, WorkerQ, PeerQ),
					get_block_trail_loop(WorkerQ2, PeerQ2, Retries2, Trail, FetchState);
				false ->
					case queue:to_list(PeerQ) of
						[Peer] -> % The last peer left and it is out of attempts.
							ar:console(
								"Failed to fetch the joining headers from any of the peers, "
								"consider trying some other trusted peers.", []),
							?LOG_ERROR([{event, failed_to_join}]),
							timer:sleep(1000),
							erlang:halt();
						_ ->
							case queue:member(Peer, PeerQ) of
								false ->
									{WorkerQ2, PeerQ2} = request_block(H, WorkerQ, PeerQ),
									get_block_trail_loop(WorkerQ2, PeerQ2, Retries, Trail,
											FetchState);
								true ->
									PeerQ2 = queue:delete(Peer, PeerQ),
									ar:console("Failed to fetch a joining block ~s from ~s. "
											"Removing the peer from the queue..",
											[ar_util:encode(H), ar_util:format_peer(Peer)]),
									?LOG_ERROR([
										{event, failed_to_fetch_joining_block},
										{block, ar_util:encode(H)},
										{peer, ar_util:format_peer(Peer)},
										{response, io_lib:format("~p", [Response])}
									]),
									{WorkerQ2, PeerQ3} = request_block(H, WorkerQ, PeerQ2),
									get_block_trail_loop(WorkerQ2, PeerQ3, Retries, Trail,
											FetchState)
							end
					end
			end;
		{tx_response, H, TXID, _Peer, #tx{} = TX} ->
			ar_disk_cache:write_tx(TX),
			{BShadow, TXMap, AwaitingTXCount} = maps:get(H, FetchState),
			TXMap2 = maps:put(TXID, TX, TXMap),
			AwaitingTXCount2 = AwaitingTXCount - 1,
			FetchState2 = maps:put(H, {BShadow, TXMap2, AwaitingTXCount2}, FetchState),
			AwaitingBlockCount = maps:get(awaiting_block_count, FetchState2),
			AwaitingBlockCount2 =
				case AwaitingTXCount2 of
					0 ->
						?LOG_INFO([{event, join_remaining_blocks_to_fetch},
								{remaining_blocks_count, AwaitingBlockCount - 1}]),
						AwaitingBlockCount - 1;
					_ ->
						AwaitingBlockCount
				end,
			FetchState3 = maps:put(awaiting_block_count, AwaitingBlockCount2, FetchState2),
			case AwaitingBlockCount2 of
				0 ->
					get_blocks(Trail, FetchState3);
				_ ->
					get_block_trail_loop(WorkerQ, PeerQ, Retries, Trail, FetchState3)
			end;
		{tx_response, H, TXID, Peer, Response} ->
			PeerRetries = maps:get(Peer, Retries),
			case PeerRetries > 0 of
				true ->
					ar:console("Failed to fetch a joining transaction ~s from ~s. "
							"Retrying..~n", [ar_util:encode(TXID), ar_util:format_peer(Peer)]),
					?LOG_WARNING([{event, failed_to_fetch_joining_tx},
							{block, ar_util:encode(H)},
							{tx, ar_util:encode(TXID)},
							{peer, ar_util:format_peer(Peer)},
							{response, io_lib:format("~p", [Response])}]),
					timer:sleep(1000),
					Retries2 = maps:put(Peer, PeerRetries - 1, Retries),
					{WorkerQ2, PeerQ2} = request_tx(H, TXID, WorkerQ, PeerQ),
					get_block_trail_loop(WorkerQ2, PeerQ2, Retries2, Trail, FetchState);
				false ->
					case queue:to_list(PeerQ) of
						[Peer] -> % The last peer left and it is out of attempts.
							ar:console(
								"Failed to fetch the joining headers from any of the peers, "
								"consider trying some other trusted peers.", []),
							?LOG_ERROR([{event, failed_to_join}]),
							timer:sleep(1000),
							erlang:halt();
						_ ->
							case queue:member(Peer, PeerQ) of
								false ->
									{WorkerQ2, PeerQ2} = request_tx(H, TXID, WorkerQ, PeerQ),
									get_block_trail_loop(WorkerQ2, PeerQ2, Retries, Trail,
											FetchState);
								true ->
									PeerQ2 = queue:delete(Peer, PeerQ),
									ar:console("Failed to fetch a joining tx ~s from ~s. "
											"Removing the peer from the queue..",
											[ar_util:encode(TXID), ar_util:format_peer(Peer)]),
									?LOG_ERROR([
										{event, failed_to_fetch_joining_tx},
										{block, ar_util:encode(H)},
										{tx, ar_util:encode(TXID)},
										{peer, ar_util:format_peer(Peer)},
										{response, io_lib:format("~p", [Response])}
									]),
									{WorkerQ2, PeerQ3} = request_tx(H, TXID, WorkerQ, PeerQ2),
									get_block_trail_loop(WorkerQ2, PeerQ3, Retries, Trail,
											FetchState)
							end
					end
			end
	end.

request_txs(_H, [], WorkerQ, PeerQ) ->
	{WorkerQ, PeerQ};
request_txs(H, [TXID | TXIDs], WorkerQ, PeerQ) ->
	{WorkerQ2, PeerQ2} = request_tx(H, TXID, WorkerQ, PeerQ),
	request_txs(H, TXIDs, WorkerQ2, PeerQ2).

request_tx(H, TXID, WorkerQ, PeerQ) ->
	{{value, W}, WorkerQ2} = queue:out(WorkerQ),
	{{value, Peer}, PeerQ2} = queue:out(PeerQ),
	W ! {get_tx, H, TXID, Peer, self()},
	{queue:in(W, WorkerQ2), queue:in(Peer, PeerQ2)}.

get_blocks([], _FetchState) ->
	[];
get_blocks([{H, _, _} | Trail], FetchState) ->
	{B, TXMap, _} = maps:get(H, FetchState),
	TXs = [maps:get(TXID, TXMap) || TXID <- B#block.txs],
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, B#block.height),
	[B#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs } | get_blocks(Trail, FetchState)].

request_block(H, WorkerQ, PeerQ) ->
	{{value, W}, WorkerQ2} = queue:out(WorkerQ),
	{{value, Peer}, PeerQ2} = queue:out(PeerQ),
	W ! {get_block_shadow, H, Peer, self()},
	{queue:in(W, WorkerQ2), queue:in(Peer, PeerQ2)}.

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

may_be_set_block_time_history([#block{ height = Height } | _] = Blocks, Peers) ->
	Fork_2_7 = ar_fork:height_2_7(),
	case Height >= Fork_2_7 of
		true ->
			Len = min(Height - Fork_2_7 + 1, ?STORE_BLOCKS_BEHIND_CURRENT),
			L = [B#block.block_time_history_hash || B <- lists:sublist(Blocks, Len)],
			case ar_http_iface_client:get_block_time_history(Peers, hd(Blocks), L) of
				{ok, BlockTimeHistory} ->
					set_block_time_history(Blocks, BlockTimeHistory);
				_ ->
					ar:console("Failed to fetch the block time history for the block ~s from "
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

worker() ->
	receive
		{get_block_shadow, H, Peer, From} ->
			From ! {block_response, H, Peer, ar_http_iface_client:get_block_shadow([Peer], H)},
			worker();
		{get_tx, H, TXID, Peer, From} ->
			From ! {tx_response, H, TXID, Peer, ar_http_iface_client:get_tx(Peer, TXID)},
			worker()
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	{timeout, 60, fun() ->
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		ar_test_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_test_node:mine(),
		ar_test_node:wait_until_height(2),
		ar_test_node:join_on_master(),
		ar_test_node:assert_slave_wait_until_height(2)
	end}.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	{timeout, 60, fun() ->
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		ar_test_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_test_node:mine(),
		ar_test_node:wait_until_height(2),
		ar_test_node:join_on_master(),
		ar_test_node:assert_slave_wait_until_height(2),
		ar_test_node:slave_mine(),
		ar_test_node:wait_until_height(3)
	end}.
