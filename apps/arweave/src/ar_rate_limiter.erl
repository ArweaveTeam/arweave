-module(ar_rate_limiter).

-behaviour(gen_server).

-export([start_link/0, throttle/2, off/0, on/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl"). % Used in ?RPM_BY_PATH.
-include_lib("arweave/include/ar_blacklist_middleware.hrl").

-record(state, {
	traces,
	off
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Hang until it is safe to make another request to the given Peer with the given Path.
%% The limits are configured in include/ar_blacklist_middleware.hrl.
throttle(Peer, Path) ->
	P = ar_http_iface_server:split_path(iolist_to_binary(Path)),
	case P of
		[<<"tx">>] ->
			%% Do not throttle transaction gossip.
			ok;
		_ ->
			case catch gen_server:call(?MODULE, {throttle, Peer, P}, infinity) of
				{'EXIT', {noproc, {gen_server, call, _}}} ->
					ok;
				{'EXIT', Reason} ->
					exit(Reason);
				_ ->
					ok
			end
	end.

%% @doc Turn rate limiting off.
off() ->
	gen_server:cast(?MODULE, turn_off).

%% @doc Turn rate limiting on.
on() ->
	gen_server:cast(?MODULE, turn_on).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{ traces = #{}, off = false }}.

handle_call({throttle, _Peer, _Path}, _From, #state{ off = true } = State) ->
	{reply, ok, State};
handle_call({throttle, Peer, Path}, From, State) ->
	gen_server:cast(?MODULE, {throttle, Peer, Path, From}),
	{noreply, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({throttle, Peer, Path, From}, State) ->
	#state{ traces = Traces } = State,
	{Type, Limit} = ?RPM_BY_PATH(Path)(),
	Now = os:system_time(millisecond),
	case maps:get({Peer, Type}, Traces, not_found) of
		not_found ->
			gen_server:reply(From, ok),
			Traces2 = maps:put({Peer, Type}, {1, queue:from_list([Now])}, Traces),
			{noreply, State#state{ traces = Traces2 }};
		{N, Trace} ->
			{N2, Trace2} = cut_trace(N, queue:in(Now, Trace), Now),
			%% The macro specifies requests per minute while the throttling window
			%% is 30 seconds.
			HalfLimit = Limit div 2,
			%% Try to approach but not hit the limit.
			case N2 + 1 > max(1, HalfLimit * 80 / 100) of
				true ->
					?LOG_DEBUG([{event, approaching_peer_rpm_limit},
							{path, Path}, {minute_limit, Limit},
							{peer, ar_util:format_peer(Peer)}, {caller, From}]),
					ar_util:cast_after(1000, ?MODULE, {throttle, Peer, Path, From}),
					{noreply, State};
				false ->
					gen_server:reply(From, ok),
					Traces2 = maps:put({Peer, Type}, {N2 + 1, Trace2}, Traces),
					{noreply, State#state{ traces = Traces2 }}
			end
	end;

handle_cast(turn_off, State) ->
	{noreply, State#state{ off = true }};

handle_cast(turn_on, State) ->
	{noreply, State#state{ off = false }};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

cut_trace(N, Trace, Now) ->
	{{value, Timestamp}, Trace2} = queue:out(Trace),
	case Timestamp < Now - ?THROTTLE_PERIOD of
		true ->
			cut_trace(N - 1, Trace2, Now);
		false ->
			{N, Trace}
	end.
