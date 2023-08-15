-module(ar_poller_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	peer,
	polling_frequency_ms,
	ref,
	pause = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	[ok] = ar_events:subscribe([node_state]),
	State = #state{ polling_frequency_ms = Config#config.polling * 1000 },
	case ar_node:is_joined() of
		true ->
			{ok, handle_node_state_initialized(State)};
		false ->
			{ok, State}
	end.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(pause, State) ->
	{noreply, State#state{ pause = true }};

handle_cast(resume, #state{ pause = false } = State) ->
	{noreply, State};
handle_cast(resume, State) ->
	Ref = make_ref(),
	gen_server:cast(self(), {poll, Ref}),
	{noreply, State#state{ pause = false, ref = Ref }};

handle_cast({poll, _Ref}, #state{ pause = true } = State) ->
	{noreply, State};
handle_cast({poll, _Ref}, #state{ peer = undefined } = State) ->
	{noreply, State#state{ pause = true }};
handle_cast({poll, Ref}, #state{ ref = Ref, peer = Peer,
		polling_frequency_ms = FrequencyMs } = State) ->
	CurrentHeight = ar_node:get_height(),
	{L, NotOnChain} = ar_block_cache:get_longest_chain_block_txs_pairs(block_cache),
	HL = [H || {H, _TXIDs} <- L],
	case NotOnChain >= 5 of
		true ->
			slow_block_application_warning(NotOnChain);
		false ->
			ok
	end,
	case ar_http_iface_client:get_recent_hash_list_diff(Peer, HL) of
		{ok, in_sync} ->
			ar_util:cast_after(FrequencyMs, self(), {poll, Ref}),
			{noreply, State};
		{ok, {H, TXIDs, BlocksOnTop}} ->
			case ar_ignore_registry:member(H) of
				true ->
					ok;
				false ->
					case BlocksOnTop >= 5 of
						true ->
							warning(Peer, behind);
						false ->
							ok
					end,
					ar_ignore_registry:add_temporary(H, 1000),
					Indices = get_missing_tx_indices(TXIDs),
					case ar_http_iface_client:get_block(Peer, H, Indices) of
						{#block{ height = Height } = B, TimeMicroseconds, _Size} ->
							case Height =< CurrentHeight - 5 of
								true ->
									warning(Peer, fork);
								false ->
									ok
							end,
							case collect_missing_transactions(B#block.txs) of
								{ok, TXs} ->
									B2 = B#block{ txs = TXs },
									ar_ignore_registry:remove_temporary(H),
									gen_server:cast(ar_poller, {block, Peer, B2, TimeMicroseconds}),
									ok;
								failed ->
									?LOG_WARNING([{event, failed_to_get_block_txs_from_peer},
											{block, ar_util:encode(H)},
											{peer, ar_util:format_peer(Peer)},
											{tx_count, length(B#block.txs)}]),
									ok
							end;
						Error ->
							ar_ignore_registry:remove_temporary(H),
							?LOG_DEBUG([{event, failed_to_fetch_block},
									{peer, ar_util:format_peer(Peer)},
									{block, ar_util:encode(H)},
									{error, io_lib:format("~p", [Error])}]),
							ok
					end
			end,
			ar_util:cast_after(FrequencyMs, self(), {poll, Ref}),
			{noreply, State};
		{error, not_found} ->
			gen_server:cast(ar_poller, {peer_out_of_sync, Peer}),
			{noreply, State#state{ pause = true }};
		{error, Reason} ->
			?LOG_DEBUG([{event, failed_to_get_recent_hash_list_diff},
					{peer, ar_util:format_peer(Peer)},
					{reason, io_lib:format("~p", [Reason])}]),
			{noreply, State#state{ pause = true }}
	end;
handle_cast({poll, _Ref}, State) ->
	{noreply, State};

handle_cast({set_peer, Peer}, #state{ ref = Ref, pause = Pause } = State) ->
	Ref2 =
		case Pause of
			true ->
				Ref3 = make_ref(),
				gen_server:cast(self(), {poll, Ref3}),
				Ref3;
			false ->
				Ref
		end,
	{noreply, State#state{ peer = Peer, pause = false, ref = Ref2 }};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {message, Msg}]),
	{noreply, State}.

handle_info({event, node_state, {initialized, _B}}, State) ->
	{noreply, handle_node_state_initialized(State)};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({gun_down, _, http, normal, _, _}, State) ->
	{noreply, State};
handle_info({gun_down, _, http, closed, _, _}, State) ->
	{noreply, State};
handle_info({gun_up, _, http}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

handle_node_state_initialized(State) ->
	Ref = make_ref(),
	gen_server:cast(self(), {poll, Ref}),
	State#state{ ref = Ref }.

get_missing_tx_indices(TXIDs) ->
	get_missing_tx_indices(TXIDs, 0).

get_missing_tx_indices([], _N) ->
	[];
get_missing_tx_indices([TXID | TXIDs], N) ->
	case ar_mempool:has_tx(TXID) of
		true ->
			get_missing_tx_indices(TXIDs, N + 1);
		false ->
			[N | get_missing_tx_indices(TXIDs, N + 1)]
	end.

slow_block_application_warning(N) ->
	ar_mining_stats:pause_performance_reports(60000),
	ar_util:terminal_clear(),
	ar:console("WARNING: there are more than ~B not yet validated blocks on the longest chain."
			" Please, double-check if you are in sync with the network and make sure your "
			"CPU computes VDF fast enough or you are connected to a VDF server."
			"~nThe node may be still mining, but console performance reports are temporarily "
			"paused.~n~n", [N]).

warning(Peer, Event) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(Peer, Config#config.peers) of
		false ->
			ok;
		true ->
			ar_mining_stats:pause_performance_reports(60000),
			EventMessage =
				case Event of
					behind ->
						"is 5 or more blocks ahead of us";
					fork ->
						"is on a fork branching off of our fork 5 or more blocks behind"
				end,
			ar_util:terminal_clear(),
			ar:console("WARNING: peer ~s ~s. "
					"Please, double-check if you are in sync with the network and "
					"make sure your CPU computes VDF fast enough or you are connected "
					"to a VDF server.~nThe node may be still mining, but console performance "
					"reports are temporarily paused.~n~n",
					[ar_util:format_peer(Peer), EventMessage])
	end.

collect_missing_transactions([#tx{} = TX | TXs]) ->
	case collect_missing_transactions(TXs) of
		failed ->
			failed;
		{ok, TXs2} ->
			{ok, [TX | TXs2]}
	end;
collect_missing_transactions([TXID | TXs]) ->
	case ar_mempool:get_tx(TXID) of
		not_found ->
			failed;
		TX ->
			collect_missing_transactions([TX | TXs])
	end;
collect_missing_transactions([]) ->
	{ok, []}.
