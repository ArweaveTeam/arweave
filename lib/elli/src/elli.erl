%% @doc: Elli acceptor manager
%%
%% This gen_server owns the listen socket and manages the processes
%% accepting on that socket. When a process waiting for accept gets a
%% request, it notifies this gen_server so we can start up another
%% acceptor.
%%
-module(elli).
-behaviour(gen_server).
-include("elli.hrl").

%% API
-export([start_link/0,
         start_link/1,
         stop/1,
         get_acceptors/1,
         get_open_reqs/1,
         get_open_reqs/2,
         set_callback/3
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type req() :: #req{}.
-export_type([req/0, body/0, headers/0]).

-record(state, {socket :: elli_tcp:socket(),
                acceptors :: non_neg_integer(),
                open_reqs :: non_neg_integer(),
                options :: [{_, _}],
                callback :: callback()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() -> start_link(?EXAMPLE_CONF).

start_link(Opts) ->
    valid_callback(required_opt(callback, Opts))
        orelse throw(invalid_callback),

    case proplists:get_value(name, Opts) of
        undefined ->
            gen_server:start_link(?MODULE, [Opts], []);
        Name ->
            gen_server:start_link(Name, ?MODULE, [Opts], [])
    end.

get_acceptors(S) ->
    gen_server:call(S, get_acceptors).

get_open_reqs(S) ->
    get_open_reqs(S, 5000).

get_open_reqs(S, Timeout) ->
    gen_server:call(S, get_open_reqs, Timeout).

set_callback(S, Callback, CallbackArgs) ->
    valid_callback(Callback) orelse throw(invalid_callback),
    gen_server:call(S, {set_callback, Callback, CallbackArgs}).

stop(S) ->
    gen_server:call(S, stop).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Opts]) ->
    %% Use the exit signal from the acceptor processes to know when
    %% they exit
    process_flag(trap_exit, true),

    Callback       = required_opt(callback, Opts),
    CallbackArgs   = proplists:get_value(callback_args, Opts),
    IPAddress      = proplists:get_value(ip, Opts, {0,0,0,0}),
    Port           = proplists:get_value(port, Opts, 8080),
    MinAcceptors   = proplists:get_value(min_acceptors, Opts, 20),

    UseSSL         = proplists:get_value(ssl, Opts, false),
    KeyFile        = proplists:get_value(keyfile, Opts),
    CertFile       = proplists:get_value(certfile, Opts),
    SockType       = case UseSSL of true -> ssl; false -> plain end,
    SSLSockOpts    = case UseSSL of
                         true -> [{keyfile, KeyFile},
                                  {certfile, CertFile}];
                         false -> [] end,

    AcceptTimeout  = proplists:get_value(accept_timeout, Opts, 10000),
    RequestTimeout = proplists:get_value(request_timeout, Opts, 60000),
    HeaderTimeout  = proplists:get_value(header_timeout, Opts, 10000),
    BodyTimeout    = proplists:get_value(body_timeout, Opts, 30000),
    MaxBodySize    = proplists:get_value(max_body_size, Opts, 1024000),

    Options = [{accept_timeout, AcceptTimeout},
               {request_timeout, RequestTimeout},
               {header_timeout, HeaderTimeout},
               {body_timeout, BodyTimeout},
               {max_body_size, MaxBodySize}],

    %% Notify the handler that we are about to start accepting
    %% requests, so it can create necessary supporting processes, ETS
    %% tables, etc.
    ok = Callback:handle_event(elli_startup, [], CallbackArgs),

    {ok, Socket} = elli_tcp:listen(SockType, Port, [binary,
                                                    {ip, IPAddress},
                                                    {reuseaddr, true},
                                                    {backlog, 32768},
                                                    {packet, raw},
                                                    {active, false}
                                                    | SSLSockOpts
                                                   ]),

    Acceptors = ets:new(acceptors, [private, set]),
    StartAcc  = fun() ->
        Pid = elli_http:start_link(self(), Socket, Options, {Callback, CallbackArgs}),
        ets:insert(Acceptors, {Pid})
    end,
    [ StartAcc() || _ <- lists:seq(1, MinAcceptors)],

    {ok, #state{socket = Socket,
                acceptors = Acceptors,
                open_reqs = 0,
                options = Options,
                callback = {Callback, CallbackArgs}}}.


handle_call(get_acceptors, _From, State) ->
    Acceptors = [Pid || {Pid} <- ets:tab2list(State#state.acceptors)],
    {reply, {ok, Acceptors}, State};

handle_call(get_open_reqs, _From, State) ->
    {reply, {ok, State#state.open_reqs}, State};

handle_call({set_callback, Callback, CallbackArgs}, _From, State) ->
    ok = Callback:handle_event(elli_reconfigure, [], CallbackArgs),
    {reply, ok, State#state{callback = {Callback, CallbackArgs}}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(accepted, State) ->
    {noreply, start_add_acceptor(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', _Pid, {error, emfile}}, State) ->
    error_logger:error_msg("No more file descriptors, shutting down~n"),
    {stop, emfile, State};

handle_info({'EXIT', Pid, normal}, State) ->
    {noreply, remove_acceptor(State, Pid)};

handle_info({'EXIT', Pid, Reason}, State) ->
    error_logger:error_msg("Elli request (pid ~p) unexpectedly "
                           "crashed:~n~p~n", [Pid, Reason]),
    {noreply, remove_acceptor(State, Pid)}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

remove_acceptor(State, Pid) ->
    ets:delete(State#state.acceptors, Pid),
    State#state{open_reqs = State#state.open_reqs - 1}.

start_add_acceptor(State) ->
    Pid = elli_http:start_link(self(), State#state.socket,
                               State#state.options, State#state.callback),
    ets:insert(State#state.acceptors, {Pid}),
    State#state{open_reqs = State#state.open_reqs + 1}.


required_opt(Name, Opts) ->
    case proplists:get_value(Name, Opts) of
        undefined ->
            throw(badarg);
        Value ->
            Value
    end.

valid_callback(Mod) ->
    lists:member({handle, 2}, Mod:module_info(exports)) andalso
        lists:member({handle_event, 3}, Mod:module_info(exports)).
