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
%%% the non-blocking `update_remaining/3'.
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
%%% %% ... issue HTTP request ...
%%% ok = arweave_client_throttling:update_remaining(general, Peer, 42).
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
	update_remaining/3,
	status/2,
	groups/0,
	reset/1
]).

-export([start/2, stop/1]).

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% Application API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Start the `arweave_client_throttling' application together
%% with its dependencies.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, term()}.
start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, _Deps} -> ok;
		Error -> Error
	end.

%%--------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
	application:stop(?MODULE).

%%--------------------------------------------------------------------
%% Throttling API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Blocking call: returns `ok' when the caller is allowed to
%% issue an outgoing request to `Peer' inside group `GroupId'.
%%
%% The call blocks via `gen_server:call(_, _, infinity)'. Callers that
%% need a timeout should wrap this function in their own bounded
%% mechanism (e.g. `proc_lib:spawn'/`receive after').
%% @end
%%--------------------------------------------------------------------
-spec throttle(atom(), tuple()) -> ok | {error, term()}.
throttle(GroupId, Peer) when is_atom(GroupId), is_tuple(Peer) ->
	arweave_client_throttling_group:throttle(GroupId, Peer).

%%--------------------------------------------------------------------
%% @doc Non-blocking refresh of the peer's remaining budget. The
%% provided value typically originates from a rate-limit response
%% header returned by the remote host. The call is implemented as a
%% `gen_server:cast' and therefore never blocks the caller.
%%
%% The throttler accepts multiple updates per peer arriving from
%% concurrent in-flight requests. When two updates fall within the
%% configured `concurrency_window_ms', the conservative minimum is
%% kept.
%% @end
%%--------------------------------------------------------------------
-spec update_remaining(atom(), tuple(), non_neg_integer()) -> ok.
update_remaining(GroupId, Peer, Remaining)
		when is_atom(GroupId), is_tuple(Peer),
			is_integer(Remaining), Remaining >= 0 ->
	arweave_client_throttling_group:update_remaining(GroupId, Peer, Remaining).

%%--------------------------------------------------------------------
%% @doc Return a snapshot of the throttler state for `Peer' in
%% `GroupId': `remaining', `queue_length', `last_update_ts'.
%% @end
%%--------------------------------------------------------------------
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupId, Peer) when is_atom(GroupId), is_tuple(Peer) ->
	arweave_client_throttling_group:status(GroupId, Peer).

%%--------------------------------------------------------------------
%% @doc Return the list of configured group ids.
%% @end
%%--------------------------------------------------------------------
-spec groups() -> [atom()].
groups() ->
	[Id || #{id := Id} <- arweave_client_throttling_config:get_groups()].

%%--------------------------------------------------------------------
%% @doc Drop the per-peer state for `GroupId' and release any blocked
%% callers with `ok'. Intended for tests and operational recovery.
%% @end
%%--------------------------------------------------------------------
-spec reset(atom()) -> ok.
reset(GroupId) when is_atom(GroupId) ->
	arweave_client_throttling_group:reset(GroupId).

%%--------------------------------------------------------------------
%% application behaviour callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
	?LOG_INFO("arweave_client_throttling application starting"),
	arweave_client_throttling_sup:start_link().

stop(_State) ->
	?LOG_INFO("arweave_client_throttling application stopped"),
	ok.
