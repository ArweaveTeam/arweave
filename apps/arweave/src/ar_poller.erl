%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc The module periodically asks peers about their recent blocks and downloads
%%% the missing ones. It serves the following purposes:
%%%
%%% - allows following the network in the absence of a public IP;
%%% - protects the node from lagging behind when there are networking issues.

-module(ar_poller).

-behaviour(gen_server).

-export([start_link/2, pause/0, resume/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% The frequency of choosing the peers to poll.
-ifdef(DEBUG).
-define(COLLECT_PEERS_FREQUENCY_MS, 2000).
-else.
-define(COLLECT_PEERS_FREQUENCY_MS, 1000 * 15).
-endif.

-record(state, {
	workers,
	worker_count,
	pause = false,
	in_sync_trusted_peers = sets:new()
}).

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Put polling on pause.
pause() ->
	gen_server:cast(?MODULE, pause).

%% @doc Resume paused polling.
resume() ->
	gen_server:cast(?MODULE, resume).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(node_state),
	case ar_node:is_joined() of
		true ->
			handle_node_state_initialized();
		false ->
			ok
	end,
	{ok, Config} = application:get_env(arweave, config),
	{ok, #state{
		workers = Workers,
		worker_count = length(Workers),
		in_sync_trusted_peers = sets:from_list(Config#config.peers)
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(pause, #state{ workers = Workers } = State) ->
	[gen_server:cast(W, pause) || W <- Workers],
	{noreply, State#state{ pause = true }};

handle_cast(resume, #state{ pause = false } = State) ->
	{noreply, State};
handle_cast(resume, #state{ workers = Workers } = State) ->
	[gen_server:cast(W, resume) || W <- Workers],
	gen_server:cast(?MODULE, collect_peers),
	{noreply, State#state{ pause = false }};

handle_cast(collect_peers, #state{ pause = true } = State) ->
	{noreply, State};
handle_cast(collect_peers, State) ->
	#state{ worker_count = N, workers = Workers } = State,
	TrustedPeers = lists:sublist(ar_peers:get_trusted_peers(), N div 3),
	Peers = ar_peers:get_peers(lifetime),
	PickedPeers = TrustedPeers ++ lists:sublist((Peers -- TrustedPeers),
			N - length(TrustedPeers)),
	start_polling_peers(Workers, PickedPeers),
	ar_util:cast_after(?COLLECT_PEERS_FREQUENCY_MS, ?MODULE, collect_peers),
	{noreply, State};

handle_cast({peer_out_of_sync_timeout, Peer}, State) ->
	#state{ in_sync_trusted_peers = Set } = State,
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(Peer, Config#config.peers) of
		false ->
			{noreply, State};
		true ->
			{noreply, State#state{ in_sync_trusted_peers = sets:add_element(Peer, Set) }}
	end;

handle_cast({peer_out_of_sync, Peer}, State) ->
	#state{ in_sync_trusted_peers = Set } = State,
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(Peer, Config#config.peers) of
		false ->
			{noreply, State};
		true ->
			Set2 = sets:del_element(Peer, Set),
			ar_util:cast_after(300000, ?MODULE, {peer_out_of_sync_timeout, Peer}),
			case {sets:is_empty(Set), sets:is_empty(Set2)} of
				{false, true} ->
					ar_mining_stats:pause_performance_reports(60000),
					ar_util:terminal_clear(),
					TrustedPeersStr = string:join([ar_util:format_peer(Peer2) ||
						Peer2 <- Config#config.peers], ", "),
					?LOG_INFO([{event, node_out_of_sync}, {peer, ar_util:format_peer(Peer)},
						{trusted_peers, TrustedPeersStr}]),
					ar:console("WARNING: The node is out of sync with all of the specified "
							"trusted peers: ~s.~n~n"
							"Please, check whether you are in sync with the network and "
							"make sure your CPU computes VDF fast enough or you are connected "
							"to a VDF server.~nThe node may be still mining, but console "
							"performance reports are temporarily paused.~n~n",
							[TrustedPeersStr]);
				_ ->
					ok
			end,
			{noreply, State#state{ in_sync_trusted_peers = Set2 }}
	end;

handle_cast({block, Peer, B, BlockQueryTime}, State) ->
	case ar_ignore_registry:member(B#block.indep_hash) of
		false ->
			?LOG_INFO([{event, fetched_block_for_validation},
					{block, ar_util:encode(B#block.indep_hash)},
					{peer, ar_util:format_peer(Peer)}]);
		true ->
			ok
	end,
	case ar_block_pre_validator:pre_validate(B, Peer, erlang:timestamp()) of
		ok ->
			ar_peers:rate_fetched_data(Peer, block, BlockQueryTime, byte_size(term_to_binary(B)));
		_ ->
			ok
	end,
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({event, node_state, {initialized, _B}}, State) ->
	handle_node_state_initialized(),
	{noreply, State};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

handle_node_state_initialized() ->
	gen_server:cast(?MODULE, collect_peers).

start_polling_peers([W | Workers], [Peer | Peers]) ->
	gen_server:cast(W, {set_peer, Peer}),
	start_polling_peers(Workers, Peers);
start_polling_peers(_Workers, []) ->
	ok.
