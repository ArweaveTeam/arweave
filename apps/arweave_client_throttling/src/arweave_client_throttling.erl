%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @doc Public interface for the Arweave client-side request
%%% throttler.
%%%
%%% Each throttling group is identified by an atom and corresponds to
%%% a registered gen_server. A caller asking to make an outgoing
%%% request invokes `throttle/2', which blocks until budget is
%%% available for the given peer. Once a response from the remote
%%% comes back, the caller is expected to report the latest budget via
%%% the non-blocking `update_quota/3'.
%%%
%%% == Peer keys ==
%%%
%%% A peer is identified by an opaque tuple, typically the IPv4
%%% 4-tuple `{A, B, C, D}' or the 5-tuple `{A, B, C, D, Port}'. Any
%%% other Erlang term that can be a map key would also work; the
%%% throttler treats the peer as opaque.
%%%
%%% == Example ==
%%%
%%% ```
%%% ok = arweave_client_throttling:start(),
%%% Peer = {127, 0, 0, 1, 1984},
%%% ok = arweave_client_throttling:throttle(general, Peer),
%%% %% ... issue HTTP request and read rate-limit headers ...
%%% ok = arweave_client_throttling:update_quota(general, Peer, #{
%%%     total => 200,
%%%     remaining => 42,
%%%     reset_seconds => 0
%%% }).
%%% '''
%%% @end
%%%===================================================================
-module(arweave_client_throttling).
-vsn(1).
-behavior(application).

-export([
    start/0,
    stop/0,
    throttle/2,
    update_quota/3,
    status/2,
    groups/0,
    reset/1
]).

-export([start/2, stop/1]).

-include_lib("kernel/include/logger.hrl").

%% @doc Start the `arweave_client_throttling' application together
%% with its dependencies.
-spec start() -> ok | {error, term()}.
start() ->
    case application:ensure_all_started(?MODULE, permanent) of
        {ok, _Deps} -> ok;
        Error ->
            ?LOG_DEBUG([{event, arweave_client_throttling_start_error},
                        {error, Error}]),
            Error
    end.

%% @doc Stop the application.
-spec stop() -> ok.
stop() ->
    application:stop(?MODULE).

%% @doc Blocking call: returns `ok' when the caller is allowed to
%% issue an outgoing request to `Peer' inside group `GroupId'.
%%
%% The gen_server itself never blocks: it replies immediately with
%% `accepted', `{queued, Ref}' or `{error, queue_full}'. In the
%% queued case `throttle/2' waits in the caller's own mailbox for a
%% `{request_ready, Ref}' notification, with a 60s ceiling - on
%% expiry it cancels the queued entry and returns `{error, timeout}'.
-spec throttle(atom(), tuple()) -> ok | {error, term()}.
throttle(Peer, Path) when is_tuple(Peer), is_list(Path) ->
    case  arweave_client_throttling_path:path_to_group_id(Path) of
        skip ->
            ok;
        GroupId ->
            arweave_client_throttling_group:throttle(GroupId, Peer)
    end.

-spec is_throttled(atom(), tuple()) -> bool().
is_throttled(Peer, Path) when is_tuple(Peer), is_list(Path) ->
    false.

%% @doc Non-blocking refresh of the peer's quota state.
%%
%% `Quota' is a map carrying the values typically extracted from the
%% rate-limit headers of a response coming back from `Peer':
%%
%% <ul>
%%   <li>`total' - full size of the quota window.</li>
%%   <li>`remaining' - how many calls are still allowed.</li>
%%   <li>`reset_seconds' - seconds until the quota refills. Used only
%%       when the quota is exhausted; pass `0' otherwise.</li>
%% </ul>
%%
%% Implemented as a `gen_server:cast' and therefore never blocks the
%% caller. The throttler accepts multiple updates per peer arriving
%% from concurrent in-flight requests; when two updates fall within
%% the configured `concurrency_window_ms', the conservative minimum
%% of the reported `remaining' is kept. `total' and `reset_seconds'
%% always take the value from the most recent update.
-spec update_quota(atom(), tuple(), map()) -> ok.
update_quota(Peer, Path, Headers) when is_tuple(Peer), is_list(Path),
                                       is_list(Headers) ->
    GroupID = arweave_client_throttling_path:path_to_group_id(Path),
    case arweave_client_throttling_http_headers:quota_from_headers(GroupID, Headers) of
        {error, Reason} ->
            ReasonStr = get_quota_error_reason(Reason),
            prometheus_counter:inc(arweave_client_throttling_quota_update_error,
                                   [atom_to_list(GroupID), ReasonStr]),
            ok;
        Quota ->
            arweave_client_throttling_group:update_quota(GroupID, Peer, Quota)
    end.

%% @doc Return a snapshot of the throttler state for `Peer' in
%% `GroupId': `total', `remaining', `reset_seconds', `queue_length',
%% `last_update_ts'.
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupId, Peer) when is_atom(GroupId), is_tuple(Peer) ->
    arweave_client_throttling_group:status(GroupId, Peer).

%% @doc Return the list of configured group ids.
-spec groups() -> [atom()].
groups() ->
    [Id || #{id := Id} <- arweave_client_throttling_config:get_groups()].

%% @doc Drop the per-peer state for `GroupId' and release any blocked
%% callers with `ok'. Intended for tests and operational recovery.
-spec reset(atom()) -> ok.
reset(GroupId) when is_atom(GroupId) ->
    arweave_client_throttling_group:reset(GroupId).

%% application behaviour callbacks
start(_StartType, _StartArgs) ->
    ?LOG_INFO("arweave_client_throttling application starting"),
    arweave_client_throttling_sup:start_link().

stop(_State) ->
    ?LOG_INFO("arweave_client_throttling application stopped"),
    ok.

%% Private
get_quota_error_reason(Reason) when is_atom(Reason) ->
    atom_to_list(Reason);
get_quota_error_reason({group_mismatch, _GroupID, _HeaderGroupID}) ->
    "group_mismatch";
get_quota_error_reason({missing_header, _HeaderKey}) ->
    "missing_header";
get_quota_error_reason(_) ->
    "unexpected".
