%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_poller).
-behaviour(gen_server).

-export([start_link/0]).

-export([
	init/1,
	handle_cast/2, handle_call/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%% This module fetches blocks from trusted peers in case the node is not in the
%%% public network or hasn't received blocks for some other reason.

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_poller_start}]),
	{ok, Config} = application:get_env(arweave, config),
	{ok, _} = schedule_polling(Config#config.polling * 1000),
	{ok, #{
		last_seen_height => -1,
		interval => Config#config.polling
	}}.

handle_cast(poll_block, State) ->
	#{
		last_seen_height := LastSeenHeight,
		interval := Interval
	} = State,
	{NewLastSeenHeight, NeedPoll} =
		case ar_node:get_height() of
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
				{ok, Config} = application:get_env(arweave, config),
				case fetch_block(Config#config.peers, NewLastSeenHeight + 1) of
					ok ->
						%% Check if we have missed more than one block.
						%% For instance, we could have missed several blocks
						%% if it took some time to join the network.
						{ok, _} = schedule_polling(2000),
						State#{ last_seen_height => NewLastSeenHeight + 1 };
					{error, _} ->
						{ok, _} = schedule_polling(Interval * 1000),
						State#{ last_seen_height => NewLastSeenHeight }
				end;
			false ->
				Delay = case NewLastSeenHeight of -1 -> 200; _ -> Interval * 1000 end,
				{ok, _} = schedule_polling(Delay),
				State#{ last_seen_height => NewLastSeenHeight }
		end,
	{noreply, NewState}.

handle_call(_Request, _From, State) ->
	{noreply, State}.

%%%===================================================================
%%% Internal functions.
%%%===================================================================

schedule_polling(Interval) ->
	timer:apply_after(Interval, gen_server, cast, [self(), poll_block]).

fetch_block(Peers, Height) ->
	case ar_http_iface_client:get_block_shadow(Peers, Height) of
		unavailable ->
			{error, block_not_found};
		{Peer, BShadow} ->
			Timestamp = erlang:timestamp(),
			case fetch_previous_blocks(Peer, BShadow, Timestamp) of
				ok ->
					ok;
				Error ->
					Error
			end
	end.

fetch_previous_blocks(Peer, BShadow, ReceiveTimestamp) ->
	HL = ar_node:get_block_anchors(),
	case fetch_previous_blocks2(Peer, BShadow, HL) of
		{ok, FetchedBlocks} ->
			submit_fetched_blocks(FetchedBlocks, Peer, ReceiveTimestamp),
			submit_fetched_blocks([BShadow], Peer, ReceiveTimestamp);
		{error, _} = Error ->
			Error
	end.

fetch_previous_blocks2(Peer, FetchedBShadow, BehindCurrentHL) ->
	fetch_previous_blocks2(Peer, FetchedBShadow, BehindCurrentHL, []).

fetch_previous_blocks2(_Peer, _FetchedBShadow, _BehindCurrentHL, FetchedBlocks)
		when length(FetchedBlocks) >= ?STORE_BLOCKS_BEHIND_CURRENT ->
	{error, failed_to_reconstruct_block_hash_list};
fetch_previous_blocks2(Peer, FetchedBShadow, BehindCurrentHL, FetchedBlocks) ->
	PrevH = FetchedBShadow#block.previous_block,
	case lists:dropwhile(fun(H) -> H /= PrevH end, BehindCurrentHL) of
		[PrevH | _] ->
			{ok, FetchedBlocks};
		_ ->
			case ar_http_iface_client:get_block_shadow([Peer], PrevH) of
				unavailable ->
					{error, previous_block_not_found};
				{_, PrevBShadow} ->
					fetch_previous_blocks2(
						Peer,
						PrevBShadow,
						BehindCurrentHL,
						[PrevBShadow | FetchedBlocks]
					)
			end
	end.

submit_fetched_blocks([B | Blocks], Peer, ReceiveTimestamp) ->
	?LOG_INFO([
		{event, ar_poller_fetched_block},
		{block, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]),
	%% Won't be broadcasted.
	ar_events:send(block, {new, B, ar_poller}),
	submit_fetched_blocks(Blocks, Peer, ReceiveTimestamp);
submit_fetched_blocks([], _Peer, _ReceiveTimestamp) ->
	ok.
