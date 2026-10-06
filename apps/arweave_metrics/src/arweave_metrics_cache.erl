%%% @doc Off-request-path renderer for the Prometheus `/metrics'.
%%% Every 15s render and cache the metrics so that GET /metrics can
%%% return quickly. Fall back to a synchronous render if the cache is
%%% not available.
-module(arweave_metrics_cache).

-behaviour(gen_server).

-export([start_link/0, lookup/1, render_now/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl"). %% FIXME: circular dependency

%% Registry we keep pre-rendered. Everything arweave emits currently
%% lives in the `default' registry; a scrape for any other
%% registry falls back to a synchronous render in the handler.
-define(CACHED_REGISTRY, default).

-define(RENDER_INTERVAL_MS, 15_000).

-record(state, {interval_ms}).

%% ===================================================================
%% API
%% ===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the pre-rendered exposition for `Registry' as a map with
%% `content_type', `identity' (plain body) and `gzip' keys, or
%% `not_cached' if the background renderer does not track it (or has not
%% produced a body yet).
lookup(Registry) ->
    persistent_term:get({?MODULE, Registry}, not_cached).

%% @doc Force a synchronous re-render. Intended for tests.
render_now() ->
    gen_server:call(?MODULE, render_now).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    %% Render as soon as we return rather than inline, so a slow or
    %% failing collector can never block or crash supervisor startup.
    %% Until the first render lands the handler serves scrapes through
    %% the synchronous fallback path.
    schedule_render(0),
    {ok, #state{interval_ms = ?RENDER_INTERVAL_MS}}.

handle_call(render_now, _From, State) ->
    render(),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(render, State) ->
    render(),
    schedule_render(State#state.interval_ms),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ===================================================================
%% Private functions
%% ===================================================================

schedule_render(DelayMs) ->
    erlang:send_after(DelayMs, self(), render).

render() ->
    Registry = ?CACHED_REGISTRY,
    try arweave_metrics_render:render(Registry) of
        {ok, Cache} ->
            persistent_term:put({?MODULE, Registry}, Cache);
        {error, Reason} ->
            %% Keep the last-good body so a transient collector failure
            %% doesn't blank the endpoint.
            ?LOG_WARNING([{event, metrics_cache_render_failed},
                          {registry, Registry}, {reason, Reason}])
    catch
        Class:ExcReason:Stacktrace ->
            ?LOG_WARNING([{event, metrics_cache_render_crashed},
                          {registry, Registry}, {class, Class}, {reason, ExcReason},
                          {stacktrace, Stacktrace}])
    end.
