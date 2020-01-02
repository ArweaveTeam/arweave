-module(ar_poller).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1]).
-export([handle_cast/2, handle_call/3]).
-export([handle_info/2]).

-include("ar.hrl").

%%% This module fetches blocks from trusted peers in case the node is not in the
%%% public network or hasn't received blocks for some other reason.

%% The polling frequency in seconds.
-define(DEFAULT_POLLING_INTERVAL, 60 * 1000).

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Args) ->
	ar:info([{event, ar_poller_start}]),
	I = proplists:get_value(polling_interval, Args, ?DEFAULT_POLLING_INTERVAL),
	{ok, _} = schedule_polling(I),
	{ok, #{
		trusted_peers => proplists:get_value(trusted_peers, Args, []),
		last_seen_height => -1,
		interval => I
	}}.

handle_cast(poll_block, State) ->
	#{ trusted_peers := TrustedPeers, last_seen_height := LastSeenHeight, interval := Interval } = State,
	{NewLastSeenHeight, NeedPoll} = case ar_node:get_height(whereis(http_entrypoint_node)) of
		-1 ->
			%% Wait until the node joins the network or starts from a hash list.
			{-1, false};
		Height when LastSeenHeight == -1 ->
			{Height, true};
		Height when Height > LastSeenHeight ->
			%% Skip this poll if the block has been already received by other means.
			{Height, false};
		_ ->
			{LastSeenHeight, true}
	end,
	NewState = case NeedPoll of
		true ->
			case poll_block(TrustedPeers, NewLastSeenHeight + 1) of
				{error, block_already_received} ->
					State#{ last_seen_height => NewLastSeenHeight + 1 };
				ok ->
					State#{ last_seen_height => NewLastSeenHeight + 1 };
				{error, _} ->
					State#{ last_seen_height => NewLastSeenHeight }
			end;
		false ->
			State#{ last_seen_height => NewLastSeenHeight }
	end,
	{ok, _} = schedule_polling(Interval),
	{noreply, NewState}.

handle_call(_Request, _From, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	%% Ignore unexpected messages. Some unexpected messages are received
	%% during startup (and sometimes later on) from ar_node
	%% after the get function times out, but the reply still arrives then.
	%% This handler only avoids a few warnings in the logs -
	%% the ar_node functions have to be changed to address the problem.
	{noreply, State}.

%%%===================================================================
%%% Internal functions.
%%%===================================================================

schedule_polling(Interval) ->
	timer:apply_after(Interval, gen_server, cast, [?MODULE, poll_block]).

poll_block(Peers, Height) ->
	poll_block_step(download_block_shadow, {Peers, Height}).

poll_block_step(download_block_shadow, {Peers, Height}) ->
	case ar_http_iface_client:get_block_shadow(Peers, Height) of
		unavailable ->
			{error, block_not_found};
		{Peer, BShadow} ->
			poll_block_step(check_ignore_list, {Peer, BShadow})
	end;
poll_block_step(check_ignore_list, {Peer, BShadow}) ->
	BH = BShadow#block.indep_hash,
	case ar_bridge:is_id_ignored(BH) of
		true ->
			{error, block_already_received};
		false ->
			ar_bridge:ignore_id(BH),
			case catch poll_block_step(construct_block_index, {Peer, BShadow}) of
				ok ->
					ok;
				Error ->
					ar_bridge:unignore_id(BH),
					Error
			end
	end;
poll_block_step(construct_block_index, {Peer, BShadow}) ->
	Node = whereis(http_entrypoint_node),
	{ok, BlockTXsPairs} = ar_node:get_block_txs_pairs(Node),
	HL = lists:map(fun({BH, _}) -> BH end, BlockTXsPairs),
	case reconstruct_block_index(Peer, BShadow, HL) of
		{ok, BI} ->
			poll_block_step(accept_block, {Peer, BShadow#block{ block_index = BI }});
		{error, _} = Error ->
			Error
	end;
poll_block_step(accept_block, {Peer, BShadow}) ->
	Node = whereis(http_entrypoint_node),
	BShadowHeight = BShadow#block.height,
	Node ! {new_block, Peer, BShadowHeight, BShadow, no_data_segment, undefined},
	ok.

reconstruct_block_index(Peer, FetchedBShadow, BehindCurrentBI) ->
	reconstruct_block_index(Peer, FetchedBShadow, BehindCurrentBI, []).

reconstruct_block_index(_Peer, _FetchedBShadow, _BehindCurrentBI, FetchedBI)
		when length(FetchedBI) >= ?STORE_BLOCKS_BEHIND_CURRENT ->
	{error, failed_to_reconstruct_block_index};
reconstruct_block_index(Peer, FetchedBShadow, BehindCurrentBI, FetchedBI) ->
	PrevH = FetchedBShadow#block.previous_block,
	case lists:dropwhile(fun({H, _}) -> H /= PrevH end, BehindCurrentBI) of
		[PrevH | _] = L ->
			{ok, lists:sublist(lists:reverse(FetchedBI) ++ L, ?STORE_BLOCKS_BEHIND_CURRENT)};
		_ ->
			case ar_http_iface_client:get_block_shadow([Peer], PrevH) of
				unavailable ->
					{error, previous_block_not_found};
				{_, PrevBShadow} ->
					reconstruct_block_index(Peer, PrevBShadow, BehindCurrentBI, [{PrevH, PrevBShadow#block.weave_size} | FetchedBI])
			end
	end.
