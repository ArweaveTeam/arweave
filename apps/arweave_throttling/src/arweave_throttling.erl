%%%===================================================================
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
%%% Throttling ledger keys are the exact peer terms supplied by callers;
%%% the throttler does not normalize them. Different tuple
%%% representations of the same IP address therefore have independent
%%% quota state.
%%%
%%% == Example ==
%%%
%%% ```
%%% ok = arweave_throttling:start(),
%%% Peer = {127, 0, 0, 1, 1984},
%%% Path = "info",
%%% ok = arweave_throttling:throttle(Peer, Path),
%%% %% ... issue HTTP request and read rate-limit headers ...
%%% ok = arweave_throttling:update_quota(Peer, Path, Headers).
%%% '''
%%% @end
%%%===================================================================
-module(arweave_throttling).
-vsn(1).
-behavior(application).

-export([
    start/0,
    stop/0,
    throttle/2,
    is_throttled/2,
    update_quota/3,
    status/2,
    reset/1
]).

-export([start/2, stop/1]).

-include_lib("kernel/include/logger.hrl").

%% Number of throttling groups a peer is allowed to have.
%% 1000 should be more than enough. I hope we can assume that a reasonable
%% user won't configure 1000 different limiter groups for their node.
-define(DISTINCT_GROUP_ID_LIMIT, 1000).

%% @doc Start the `arweave_throttling' application together
%% with its dependencies.
-spec start() -> ok | {error, term()}.
start() ->
    case application:ensure_all_started(?MODULE, permanent) of
        {ok, _Deps} -> ok;
        Error ->
            ?LOG_ERROR([{event, arweave_throttling_start_error},
                        {error, Error}]),
            Error
    end.

%% @doc Stop the application.
-spec stop() -> ok.
stop() ->
    application:stop(?MODULE).

%% @doc Blocking call: returns `ok' when the caller is allowed to
%% issue an outgoing request to `Peer' for `Path'.
%%
%% The gen_server itself never blocks: it replies immediately with
%% `accepted', `{queued, Ref}' or `{error, queue_full}'. In the
%% queued case `throttle/2' waits in the caller's own mailbox for a
%% `{request_ready, Ref}' notification, with a 60s ceiling - on
%% expiry it cancels the queued entry and returns
%% `{error, throttle_receive_timeout}'.
-spec throttle(tuple(), list()) -> ok | {error, term()}.
throttle(Peer, Path) when is_tuple(Peer), is_list(Path) ->
    case arweave_throttling_path:path_to_group_id(Peer, Path) of
        {error, skip} ->
            ok;
        {error, unknown_key} ->
            %% This is okay. We haven't seen requests for this {Peer, Path}, so
            %% we wouldn't know how to throttle the requests - what group it
            %% belongs to and what is the quota for that group.
            %% This might allow requests through, and be rejected when the quota
            %% is low and we are in a burst (like startups), but the worst thing
            %% can happen is getting rejected by the remote peer.
            %% Rejection is part of life. ¯\_(ツ)_/¯
            ok;
        {ok, GroupID} ->
            arweave_throttling_group:throttle(GroupID, Peer)
    end.

%% @doc Return true when this node should avoid selecting `Peer' for
%% `Path' because its outbound quota is near exhaustion.
-spec is_throttled(tuple(), list()) -> boolean().
is_throttled(Peer, Path) when is_tuple(Peer), is_list(Path) ->
    case arweave_throttling_path:path_to_group_id(Peer, Path) of
        {error, skip} ->
            false;
        {error, _ } ->
            false;
        {ok, GroupID} ->
            arweave_throttling_group:is_throttled(GroupID, Peer)
    end.

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
-spec update_quota(tuple(), list(), map()) -> ok.
update_quota(Peer, Path, Headers) when is_tuple(Peer),
                        is_list(Path) ->
    case arweave_throttling_http_headers:parse(Headers) of
        {error, {missing_header, _Key} = Reason} = E ->
            %% Missing header should reset peer state across all
            %% groups. We don't know at this point what groups the peer was a part
            %% of.
            case arweave_throttling_peer_compatibility:is_peer_marked_compatible(Peer) of
                false ->
                    %% When it's marked incompatibl already, we're good. we don't have to
                    %% do anything.
                    ok;
                true ->
                    %% Reset
                    arweave_throttling_sup:reset_peer_in_all_groups(Peer),
                    maybe_log_update_error(Peer, Path, 'unknown', Reason),
                    arweave_throttling_peer_compatibility:mark_incompatible(Peer)
            end,
            E;
        {error, Reason} = E ->
            %% We can be more tolerant towards other errors, no reset.
            %% Log with unknown group, and return, there is nothing to update.
            %% and likely never was or will be. We assume the error is consistent,
            %% as the peer runs an incompatible version.
            maybe_log_update_error(Peer, Path, 'unknown', Reason),
            E;
        {ok, #{group_id := HeaderGroupID} = Quota} ->
            %% Try to look up group ID for the Peer and Path.
            arweave_throttling_peer_compatibility:mark_compatible(Peer),
            case arweave_throttling_path:path_to_group_id(Peer, Path) of
                {error, skip} ->
                    ok;
                {error, unknown_key} ->
                    %% We received group quota information for a {Peer, Path} pair we
                    %% haven't seen before. GroupID might have been seen for other Paths
                    %% for this Peer.
                    case try_group_id_to_atom(Peer, HeaderGroupID) of
                        {error, Reason} = E ->
                            maybe_log_update_error(Peer, Path, 'unknown', Reason),
                            E;
                        {ok, HeaderGroupIDAtom} ->
                            %% Then we can look for the throttling group process.
                            case get_or_start_throttling_group_process(HeaderGroupIDAtom) of
                                {ok, Pid} when is_pid(Pid) ->
                                    handle_update_group_id(Peer, Path, HeaderGroupIDAtom, Quota);
                                {error, Reason} = E ->
                                    maybe_log_update_error(Peer, Path, HeaderGroupIDAtom, Reason),
                                    E
                            end
                    end;
                {ok, GroupID} ->
                    case HeaderGroupID =:= atom_to_binary(GroupID) of
                        true ->
                            handle_update_group_id(Peer, Path, GroupID, Quota);
                        false ->
                            E = {group_mismatch, GroupID, HeaderGroupID},
                            maybe_log_update_error(Peer, Path, 'unknown', E),
                            E
                    end
            end
    end.

%% @doc Return a snapshot of the throttler state for `Peer' in
%% `GroupID': `total', `remaining', `reset_seconds', `queue_length',
%% `last_update_ts'.
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupID, Peer) when is_atom(GroupID), is_tuple(Peer) ->
    arweave_throttling_group:status(GroupID, Peer).

%% @doc Drop the per-peer state for `GroupID' and release any blocked
%% callers with `ok'. Intended for tests and operational recovery.
-spec reset(atom()) -> ok.
reset(GroupID) when is_atom(GroupID) ->
    arweave_throttling_group:reset(GroupID).

%% application behaviour callbacks
start(_StartType, _StartArgs) ->
    ?LOG_INFO("arweave_throttling application starting"),
    ok = arweave_throttling_metrics:register(),
    ok = arweave_throttling_router:init(),
    ok = arweave_throttling_distinct_group:init(),
    ok = arweave_throttling_peer_compatibility:init(),
    S = arweave_throttling_sup:start_link(),
    prometheus_registry:register_collector(arweave_throttling_metrics_collector),
    S.

stop(_State) ->
    ok = arweave_throttling_metrics:cleanup(),
    ?LOG_INFO("arweave_throttling application stopped"),
    ok.

%% Private
try_group_id_to_atom(Peer, HeaderGroupID) ->
    %% First let's check if we saw this GroupID for this Peer, perhaps
    %% for other Paths.
    case arweave_throttling_distinct_group:is_stored(Peer, HeaderGroupID) of
        {ok, true} ->
            %% Yes, It was seen before, so the atom is safe to use.
            {ok, binary_to_atom(HeaderGroupID)};
        {ok, false} ->
            %% We limit the number of GroupIDs a peer can submit, to avoid
            %% a malicious peer spamming new GroupIDs until we run out of
            %% memory or possible atoms.
            case arweave_throttling_distinct_group:distinct_count(Peer) of
                {ok, Count} when Count >= ?DISTINCT_GROUP_ID_LIMIT ->
                    %% This is a problem, the Peer has posted too many GroupIDs.
                    %% We have to limit this number be, so we don't create too many atoms.
                    {too_many_groups, HeaderGroupID};
                {error, _Reason} = E ->
                    E;
                {ok, _Count} ->
                    %% Limit wasn't breached, it's a new GroupID, let's store it.
                    {ok, _} = arweave_throttling_distinct_group:insert(Peer, HeaderGroupID),
                    {ok, binary_to_atom(HeaderGroupID)}
            end
    end.

%% @doc Increase prometheus counters for common errors, 
maybe_log_update_error(Peer, Path, GroupID, Reason) ->
    ReasonStr = get_quota_error_reason(Reason),
    log_unknown_reason(Peer, Path, GroupID, Reason, ReasonStr),
    arweave_metrics:counter_inc(arweave_throttling_quota_update_error,
                                [atom_to_list(GroupID),
                                 ReasonStr
                                ]),
    ok.

handle_update_group_id(Peer, Path, GroupID, Quota) ->
    PathKey = arweave_throttling_path:path_to_path_key(Path),
    %% PathKey
    arweave_throttling_router:update_path(Peer, PathKey, GroupID),
    arweave_throttling_group:update_quota(GroupID, Peer, Quota).

get_or_start_throttling_group_process(GroupID) ->
    case whereis(arweave_throttling_group:registered_name(GroupID)) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        _ ->
            case arweave_throttling_sup:start_throttling_group(GroupID) of
                {ok, Child} ->
                    {ok, Child};
                {ok, Child, _Info} ->
                    {ok, Child};
                {error, _Reason} = E ->
                    E
            end
    end.

get_quota_error_reason(Reason) when is_atom(Reason) ->
    atom_to_list(Reason);
get_quota_error_reason({too_many_groups, _HeaderGroupID}) ->
    "too_many_groups";
get_quota_error_reason({group_mismatch, _GroupID, _HeaderGroupID}) ->
    "group_mismatch";
get_quota_error_reason({missing_header, _HeaderKey}) ->
    "missing_header";
get_quota_error_reason(_) ->
    "unexpected".

log_unknown_reason(Peer, Path, GroupID, Reason, "unexpected") ->
    ?LOG_ERROR([{event, update_quota_unexpected_error},
                {reason, Reason},
                {peer, Peer},
                {path, Path},
                {group_id, GroupID}]);
log_unknown_reason(_, _, _, _, _) ->
    ok.
