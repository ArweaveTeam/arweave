%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Kristof Hetzl
%%% @doc Arweave Rate Limiter.
%%%
%%% `arweave_limiter' module is an interface to the Arweave
%%% Rate Limiter functionality.
%%%
%%% @end
%%%===================================================================
-module(arweave_limiter).
-vsn(1).
-behavior(application).
-export([
         start/0,
         start/2,
         stop/0,
         stop/1
        ]).

-export([register_or_reject_call/2, reduce_for_peer/2]).

-include_lib("kernel/include/logger.hrl").


%%--------------------------------------------------------------------
%% @doc helper function to start `arweave_limiter' application.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, term()}.

start() ->
    case application:ensure_all_started(?MODULE, permanent) of
        {ok, Dependencies} ->
            ?LOG_DEBUG("arweave_limiter started dependencies: ~p", [Dependencies]),
            ok;
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc Application API function to start `arweave_config' app.
%% @end
%%--------------------------------------------------------------------
-spec start(term(), term()) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
    arweave_limiter_sup:start_link().

%%--------------------------------------------------------------------
%% @doc help function to stop `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.

stop() ->
    application:stop(?MODULE).

%%--------------------------------------------------------------------
%% @doc help function to stop `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% @doc Rate limit request
%% @end
%%--------------------------------------------------------------------
register_or_reject_call(LimiterRef, Peer) ->
    arweave_limiter_group:register_or_reject_call(LimiterRef, Peer).


%%--------------------------------------------------------------------
%% @doc Reduce leaky tokens for peer.
%% @end
%%--------------------------------------------------------------------
reduce_for_peer(LimiterRef, Peer) ->
    arweave_limiter_group:reduce_for_peer(LimiterRef, Peer).
