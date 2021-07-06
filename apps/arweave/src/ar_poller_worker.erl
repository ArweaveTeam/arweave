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
	case ar_http_iface_client:get_recent_hash_list_diff(Peer) of
		{ok, in_sync} ->
			ar_util:cast_after(FrequencyMs, self(), {poll, Ref}),
			{noreply, State};
		{ok, {H, TXIDs}} ->
			case ar_ignore_registry:member(H) of
				true ->
					ok;
				false ->
					ar_ignore_registry:add_temporary(H, 1000),
					Indices = get_missing_tx_indices(TXIDs),
					case ar_http_iface_client:get_block(Peer, H, Indices) of
						{B, Time, Size} ->
							case collect_missing_transactions(B#block.txs) of
								{ok, TXs} ->
									B2 = B#block{ txs = TXs },
									ar_ignore_registry:remove_temporary(H),
									ar_events:send(block, {discovered, Peer, B2, Time, Size}),
									ok;
								failed ->
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
		{error, node_state_not_initialized} ->
			{noreply, State};
		{error, Reason} ->
			case ar_node:get_height() >= ar_fork:height_2_6() of
				true ->
					ar_events:send(peer,
							{bad_response, {Peer, recent_hash_list_diff, Reason}}),
					?LOG_WARNING([{event, failed_to_get_recent_hash_list_diff},
							{peer, ar_util:format_peer(Peer)},
							{reason, io_lib:format("~p", [Reason])}]);
				false ->
					ok
			end,
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
	?LOG_ERROR([{event, unhandled_info}, {info, Info}]),
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
	case ets:member(node_state, {tx, TXID}) of
		true ->
			get_missing_tx_indices(TXIDs, N + 1);
		false ->
			[N | get_missing_tx_indices(TXIDs, N + 1)]
	end.

collect_missing_transactions([#tx{} = TX | TXs]) ->
	case collect_missing_transactions(TXs) of
		failed ->
			failed;
		{ok, TXs2} ->
			{ok, [TX | TXs2]}
	end;
collect_missing_transactions([TXID | TXs]) ->
	case ets:lookup(node_state, {tx, TXID}) of
		[] ->
			failed;
		[{_, TX}] ->
			collect_missing_transactions([TX | TXs])
	end;
collect_missing_transactions([]) ->
	{ok, []}.
