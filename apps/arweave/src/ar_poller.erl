%% This Source Code Form is subject to the terms of the GNU General 
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed 
%% with this file, You can obtain one at 
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
%% @author Jon Sherry
%% @author Martin Torhage
%% @author Lev Berman <lev@arweave.org>
%% @author Taras Halturin <taras@arweave.org>
%%

-module(ar_poller).
-behaviour(gen_server).

-export([start_link/1]).

-export([
	init/1,
	handle_cast/2, handle_call/3
]).

-include("ar.hrl").
-include("ar_config.hrl").

%%% This module fetches blocks from trusted peers in case the node is not in the
%%% public network or hasn't received blocks for some other reason.


%%%===================================================================
%%% Public API.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(_Args) ->
	ar:info([{event, ar_poller_start}]),

    {ok, Config} = application:get_env(arweave, config),
	
	{ok, #{
		trusted_peers => ar_join:filter_peer_list(Config#config.peers),
		last_seen_height => -1,
		interval => schedule_polling(Config#config.polling * 1000)
	}}.

handle_cast(poll_block, State) ->
	#{
		trusted_peers := TrustedPeers,
		last_seen_height := LastSeenHeight,
		interval := Interval
	} = State,
	{NewLastSeenHeight, NeedPoll} =
		case ar_node:get_height(whereis(http_entrypoint_node)) of
			-1 ->
				%% Wait until the node joins the network or starts from a hash list.
				{-1, false};
			Height when LastSeenHeight == -1 ->
				{Height, true};
			Height when Height > LastSeenHeight ->
				%% Skip this poll if the block has been already received by other means.
				%% Under normal circumstances, we never poll.
				{Height, false};
			_ ->
				{LastSeenHeight, true}
		end,
	NewState =
		case NeedPoll of
			true ->
				case poll_block(TrustedPeers, NewLastSeenHeight + 1) of
					{error, block_already_received} ->
						{ok, _} = schedule_polling(Interval),
						State#{ last_seen_height => NewLastSeenHeight + 1 };
					ok ->
						%% Check if we have missed more than one block.
						%% For instance, we could have missed several blocks
						%% if it took some time to join the network.
						{ok, _} = schedule_polling(0),
						State#{ last_seen_height => NewLastSeenHeight + 1 };
					{error, _} ->
						{ok, _} = schedule_polling(Interval),
						State#{ last_seen_height => NewLastSeenHeight }
				end;
			false ->
				Delay = case NewLastSeenHeight of -1 -> 200; _ -> Interval end,
				{ok, _} = schedule_polling(Delay),
				State#{ last_seen_height => NewLastSeenHeight }
		end,
	{noreply, NewState}.

handle_call(_Request, _From, State) ->
	{noreply, State}.

%%%===================================================================
%%% Internal functions.
%%%===================================================================

schedule_polling(0) ->
    schedule_polling(?DEFAULT_POLLING_INTERVAL);
schedule_polling(Interval) ->
	timer:apply_after(Interval, gen_server, cast, [self(), poll_block]),
    Interval.

poll_block(Peers, Height) ->
	poll_block_step(download_block_shadow, {Peers, Height}).

poll_block_step(download_block_shadow, {Peers, Height}) ->
	case ar_http_iface_client:get_block_shadow(Peers, Height) of
		unavailable ->
			{error, block_not_found};
		{Peer, BShadow} ->
			poll_block_step(check_ignore_list, {Peer, BShadow}, erlang:timestamp())
	end.

poll_block_step(check_ignore_list, {Peer, BShadow}, Timestamp) ->
	BH = BShadow#block.indep_hash,
	case ar_bridge:is_id_ignored(BH) of
		true ->
			{error, block_already_received};
		false ->
			ar_bridge:ignore_id(BH),
			case catch poll_block_step(construct_hash_list, {Peer, BShadow}, Timestamp) of
				ok ->
					ok;
				Error ->
					ar_bridge:unignore_id(BH),
					Error
			end
	end;
poll_block_step(construct_hash_list, {Peer, BShadow}, ReceiveTimestamp) ->
	Node = whereis(http_entrypoint_node),
	{ok, BlockTXsPairs} = ar_node:get_block_txs_pairs(Node),
	HL = lists:map(fun({BH, _}) -> BH end, BlockTXsPairs),
	case reconstruct_block_hash_list(Peer, BShadow, HL) of
		{ok, FetchedBlocks, BHL} ->
			lists:foreach(
				fun(B) ->
					Node ! {new_block, Peer, B#block.height, B, no_data_segment, ReceiveTimestamp}
				end,
				FetchedBlocks
			),
			BShadowHeight = BShadow#block.height,
			BShadow2 = BShadow#block{ hash_list = BHL },
			Node ! {new_block, Peer, BShadowHeight, BShadow2, no_data_segment, ReceiveTimestamp},
			ok;
		{error, _} = Error ->
			Error
	end.

reconstruct_block_hash_list(Peer, FetchedBShadow, BehindCurrentHL) ->
	reconstruct_block_hash_list(Peer, FetchedBShadow, BehindCurrentHL, []).

reconstruct_block_hash_list(_Peer, _FetchedBShadow, _BehindCurrentHL, FetchedBlocks)
		when length(FetchedBlocks) >= ?STORE_BLOCKS_BEHIND_CURRENT ->
	{error, failed_to_reconstruct_block_hash_list};
reconstruct_block_hash_list(Peer, FetchedBShadow, BehindCurrentHL, FetchedBlocks) ->
	PrevH = FetchedBShadow#block.previous_block,
	case lists:dropwhile(fun(H) -> H /= PrevH end, BehindCurrentHL) of
		[PrevH | _] = L ->
			FetchedHL = [B#block.indep_hash || B <- FetchedBlocks],
			{ok, FetchedBlocks,
				lists:sublist(lists:reverse(FetchedHL) ++ L, ?STORE_BLOCKS_BEHIND_CURRENT)};
		_ ->
			case ar_http_iface_client:get_block_shadow([Peer], PrevH) of
				unavailable ->
					{error, previous_block_not_found};
				{_, PrevBShadow} ->
					reconstruct_block_hash_list(
						Peer, PrevBShadow, BehindCurrentHL, [PrevBShadow | FetchedBlocks])
			end
	end.
