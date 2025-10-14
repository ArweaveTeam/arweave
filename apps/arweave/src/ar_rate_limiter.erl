-module(ar_rate_limiter).

-behaviour(gen_server).

-export([start_link/0, throttle/2, off/0, on/0, is_on_cooldown/2, set_cooldown/3]).
-export([is_throttled/2]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl"). % Used in ?RPM_BY_PATH.
-include_lib("arweave/include/ar_blacklist_middleware.hrl").
-include_lib("eunit/include/eunit.hrl").

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
	{ok, Config} = arweave_config:get_env(),
	case lists:member(Peer, Config#config.local_peers) of
		true ->
			ok;
		false ->
			throttle2(Peer, Path)
	end.

throttle2(Peer, Path) ->
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

%% @doc Return true if Peer should be throttled for the given RPMKey.
is_throttled(Peer, Path) ->
	case catch gen_server:call(?MODULE, {is_throttled, Peer, Path}, infinity) of
		{'EXIT', {noproc, {gen_server, call, _}}} -> false;
		{'EXIT', Reason} -> exit(Reason);
		Bool when is_boolean(Bool) -> Bool
	end.

%% @doc Return true if Peer is on cooldown for the given Path.
is_on_cooldown(Peer, RPMKey) ->
	Now = os:system_time(millisecond),
	case ets:lookup(?MODULE, {cooldown, Peer, RPMKey}) of
		[{_, Until}] when Until > Now -> true;
		_ -> false
	end.

%% @doc Put Peer on cooldown for the given RPMKey for Milliseconds.
set_cooldown(Peer, RPMKey, Milliseconds) when Milliseconds > 0 ->
	Until = os:system_time(millisecond) + Milliseconds,
	ets:insert(?MODULE, {{cooldown, Peer, RPMKey}, Until}),
	ok;
set_cooldown(_Peer, _RPMKey, _Milliseconds) ->
	ok.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{ traces = #{}, off = false }}.

handle_call({throttle, _Peer, _Path}, _From, #state{ off = true } = State) ->
	{reply, ok, State};

handle_call({is_throttled, Peer, Path}, _From, State) ->
	{Throttle, _} = is_throttled(Peer, Path, State),
	{reply, Throttle, State};
handle_call({throttle, Peer, Path}, From, State) ->
	gen_server:cast(?MODULE, {throttle, Peer, Path, From}),
	{noreply, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.


handle_cast({throttle, Peer, Path, From}, State) ->
	#state{ traces = Traces } = State,
	{RPMKey, Limit} = ?RPM_BY_PATH(Path)(),
	{Throttle, {N, Trace}} = is_throttled(Peer, Path, State),
	case Throttle of
		true ->
			?LOG_DEBUG([{event, approaching_peer_rpm_limit},
					{path, Path}, {minute_limit, Limit},
					{peer, ar_util:format_peer(Peer)}, {caller, From}]),
			ar_util:cast_after(1000, ?MODULE, {throttle, Peer, Path, From}),
			{noreply, State};
		false ->
			gen_server:reply(From, ok),
			Traces2 = maps:put({Peer, RPMKey}, {N, Trace}, Traces),
			{noreply, State#state{ traces = Traces2 }}
	end;

handle_cast(turn_off, State) ->
	{noreply, State#state{ off = true }};

handle_cast(turn_on, State) ->
	{noreply, State#state{ off = false }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
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

%% @doc Internal predicate used by both server and tests. Returns {Throttle, {NewN, NewTrace}}.
is_throttled(Peer, Path, #state{ traces = Traces } = _State) ->
	{RPMKey, Limit} = ?RPM_BY_PATH(Path)(),
	Now = os:system_time(millisecond),
	case maps:get({Peer, RPMKey}, Traces, not_found) of
		not_found ->
			{false, {1, queue:from_list([Now])}};
		{N, Trace} ->
			{N2, Trace2} = cut_trace(N, queue:in(Now, Trace), Now),
			%% The macro specifies requests per minute while the throttling window
			%% is 30 seconds.
			HalfLimit = Limit div 2,
			%% Try to approach but not hit the limit.
			Throttle = N2 + 1 > max(1, HalfLimit * 80 div 100),
			{Throttle, {N2 + 1, Trace2}}
	end.


%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

is_throttled_server_down_test() ->
	%% When the server is not running, we should not crash and return false.
	Peer = {127,0,0,1},
	?assertEqual(false, is_throttled(Peer, [<<"hash_list">>]) ).

is_throttled_test() ->
	Peer = {127,0,0,1},
	RPMKey = data_sync_record,
	Path = [<<"data_sync_record">>],
	Now = os:system_time(millisecond),
	ThrottleLimit = (?DEFAULT_REQUESTS_PER_MINUTE_LIMIT div 2) * 80 div 100,

	%% Build a trace representing ThrottleLimit - 1 requests
	Trace = queue:from_list(lists:duplicate(ThrottleLimit - 1, Now + 2000 - ?THROTTLE_PERIOD)),

	State = #state{ traces = #{ {Peer, RPMKey} => {ThrottleLimit - 1, Trace} }, off = false },
	{Throttle1, {N1, Trace1}} = is_throttled(Peer, Path, State),
	?assertEqual(false, Throttle1),

	%% Add one more implied request (same inputs) should be throttled next time
	State2 = #state{ traces = #{ {Peer, RPMKey} => {N1, Trace1} }, off = false },
	{Throttle2, {_N2, _Trace2}} = is_throttled(Peer, Path, State2),
	?assertEqual(true, Throttle2),

	%% Sleep to let most of the requests age out. Note: ar_rate_limiter only updates the traces
	%% state when there is no throttle, so we won't use {N2, Trace2}
	timer:sleep(3000),
	{Throttle3, {N3, _Trace3}} = is_throttled(Peer, Path, State2),
	?assertEqual(false, Throttle3),
	?assertEqual(2, N3),

	%% Not found path should not throttle and should suggest initial trace
	State4 = #state{ traces = #{}, off = false },
	{Throttle4, {N4, Trace4}} = is_throttled(Peer, Path, State4),
	?assertEqual(false, Throttle4),
	?assertEqual(1, N4),
	?assertEqual(1, queue:len(Trace4)),
	ok.