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
-include("elli_util.hrl").

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

-export_type([req/0, http_method/0, body/0, headers/0, response_code/0]).

%% @type req(). A record representing an HTTP request.
-type req() :: #req{}.

%% @type http_method(). An uppercase atom representing a known HTTP verb or a
%% binary for other verbs.
-type http_method() :: 'OPTIONS' | 'GET' | 'HEAD' | 'POST'
                     | 'PUT' | 'DELETE' | 'TRACE' | binary().

%% @type body(). A binary or iolist.
-type body() :: binary() | iolist().

-type header()  :: {Key::binary(), Value::binary() | string()}.
-type headers() :: [header()].

-type response_code() :: 100..999.

-record(state, {socket         :: elli_tcp:socket(),
                acceptors      :: ets:tid(),
                open_reqs = 0  :: non_neg_integer(),
                options   = [] :: [{_, _}],     % TODO: refine
                callback       :: elli_handler:callback()
               }).
%% @type state(). Internal state.
-opaque state() :: #state{}.
-export_type([state/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> Result when
      Result :: {ok, Pid} | ignore | {error, Error},
      Pid    :: pid(),
      Error  :: {already_started, Pid} | term().
%% @equiv start_link({@EXAMPLE_CONF})
%% @doc Create an Elli server process as part of a supervision tree, using the
%% default configuration.
start_link() -> start_link(?EXAMPLE_CONF).

-spec start_link(Opts) -> Result when
      Opts   :: [{_, _}],                         % TODO: refine
      Result :: {ok, Pid} | ignore | {error, Error},
      Pid    :: pid(),
      Error  :: {already_started, Pid} | term().
start_link(Opts) ->
    valid_callback(required_opt(callback, Opts))
        orelse throw(invalid_callback),

    case proplists:get_value(name, Opts) of
        undefined ->
            gen_server:start_link(?MODULE, [Opts], []);
        Name ->
            gen_server:start_link(Name, ?MODULE, [Opts], [])
    end.

-spec get_acceptors(atom()) -> {reply, {ok, [ets:tid()]}, state()}.
get_acceptors(S) ->
    gen_server:call(S, get_acceptors).

-spec get_open_reqs(S :: atom()) -> {reply, {ok, non_neg_integer()}, state()}.
%% @equiv get_open_reqs(S, 5000)
get_open_reqs(S) ->
    get_open_reqs(S, 5000).

-spec get_open_reqs(S :: atom(), Timeout :: non_neg_integer()) -> Reply when
      Reply :: {reply, {ok, non_neg_integer()}, state()}.
get_open_reqs(S, Timeout) ->
    gen_server:call(S, get_open_reqs, Timeout).

-spec set_callback(S, Callback, CallbackArgs) -> Reply when
      S            :: atom(),
      Callback     :: elli_handler:callback_mod(),
      CallbackArgs :: elli_handler:callback_args(),
      Reply        :: {reply, ok, state()}.
set_callback(S, Callback, CallbackArgs) ->
    valid_callback(Callback) orelse throw(invalid_callback),
    gen_server:call(S, {set_callback, Callback, CallbackArgs}).

%% @doc Stop `Server'.
-spec stop(Server :: atom()) -> {stop, normal, ok, state()}.
stop(S) ->
    gen_server:call(S, stop).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
-spec init([Opts :: [{_, _}]]) -> {ok, state()}.
init([Opts]) ->
    %% Use the exit signal from the acceptor processes to know when
    %% they exit
    process_flag(trap_exit, true),

    Callback       = required_opt(callback, Opts),
    CallbackArgs   = proplists:get_value(callback_args, Opts),
    IPAddress      = proplists:get_value(ip, Opts, {0, 0, 0, 0}),
    Port           = proplists:get_value(port, Opts, 8080),
    MinAcceptors   = proplists:get_value(min_acceptors, Opts, 20),

    UseSSL         = proplists:get_value(ssl, Opts, false),
    KeyFile        = proplists:get_value(keyfile, Opts),
    CertFile       = proplists:get_value(certfile, Opts),
    SockType       = ?IF(UseSSL, ssl, plain),
    SSLSockOpts    = ?IF(UseSSL,
                         [{keyfile, KeyFile}, {certfile, CertFile}],
                         []),

    AcceptTimeout  = proplists:get_value(accept_timeout, Opts, 10000),
    RequestTimeout = proplists:get_value(request_timeout, Opts, 60000),
    HeaderTimeout  = proplists:get_value(header_timeout, Opts, 10000),
    BodyTimeout    = proplists:get_value(body_timeout, Opts, 30000),
    MaxBodySize    = proplists:get_value(max_body_size, Opts, 1024000),

    Options        = [{accept_timeout, AcceptTimeout},
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
                                                    | SSLSockOpts]),

    Acceptors = ets:new(acceptors, [private, set]),
    [begin
         Pid = elli_http:start_link(self(), Socket, Options,
                                    {Callback, CallbackArgs}),
         ets:insert(Acceptors, {Pid})
     end
     || _ <- lists:seq(1, MinAcceptors)],

    {ok, #state{socket    = Socket,
                acceptors = Acceptors,
                open_reqs = 0,
                options   = Options,
                callback  = {Callback, CallbackArgs}}}.


%% @hidden
-spec handle_call(get_acceptors, {pid(), _Tag}, state()) ->
                         {reply, {ok, [ets:tid()]}, state()};
                 (get_open_reqs, {pid(), _Tag}, state()) ->
                         {reply, {ok, OpenReqs :: non_neg_integer()}, state()};
                 (stop, {pid(), _Tag}, state()) -> {stop, normal, ok, state()};
                 ({set_callback, Mod, Args}, {pid(), _Tag}, state()) ->
                         {reply, ok, state()} when
      Mod  :: elli_handler:callback_mod(),
      Args :: elli_handler:callback_args().
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

%% @hidden
-spec handle_cast(accepted | _Msg, State0) -> {noreply, State1} when
      State0 :: state(),
      State1 :: state().
handle_cast(accepted, State) ->
    {noreply, start_add_acceptor(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.


%% @hidden
-spec handle_info({'EXIT', _Pid, Reason}, State0) -> Result when
      State0 :: state(),
      Reason :: {error, emfile},
      Result :: {stop, emfile, State0}
              | {noreply, State1 :: state()}.
handle_info({'EXIT', _Pid, {error, emfile}}, State) ->
    ?LOG_ERROR("No more file descriptors, shutting down~n"),
    {stop, emfile, State};

handle_info({'EXIT', Pid, normal}, State) ->
    {noreply, remove_acceptor(State, Pid)};

handle_info({'EXIT', Pid, Reason}, State) ->
    ?LOG_ERROR("Elli request (pid ~p) unexpectedly crashed:~n~p~n", [Pid, Reason]),
    {noreply, remove_acceptor(State, Pid)}.


%% @hidden
-spec terminate(_Reason, _State) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @hidden
-spec code_change(_OldVsn, State, _Extra) -> {ok, State} when State :: state().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec remove_acceptor(State0 :: state(), Pid :: pid()) -> State1 :: state().
remove_acceptor(State, Pid) ->
    ets:delete(State#state.acceptors, Pid),
    dec_open_reqs(State).

-spec start_add_acceptor(State0 :: state()) -> State1 :: state().
start_add_acceptor(State) ->
    Pid = elli_http:start_link(self(), State#state.socket,
                               State#state.options, State#state.callback),
    add_acceptor(State, Pid).

-spec add_acceptor(State0 :: state(), Pid :: pid()) -> State1 :: state().
add_acceptor(#state{acceptors = As} = State, Pid) ->
    ets:insert(As, {Pid}),
    inc_open_reqs(State).

-spec required_opt(Name, Opts) -> Value when
      Name  :: any(),
      Opts  :: proplists:proplist(),
      Value :: term().
required_opt(Name, Opts) ->
    case proplists:get_value(Name, Opts) of
        undefined ->
            throw(badarg);
        Value ->
            Value
    end.

-spec valid_callback(Mod :: module()) -> Exported :: boolean().
valid_callback(Mod) ->
    lists:member({handle, 2}, Mod:module_info(exports)) andalso
        lists:member({handle_event, 3}, Mod:module_info(exports)).

-spec dec_open_reqs(State0 :: state()) -> State1 :: state().
dec_open_reqs(#state{open_reqs = OpenReqs} = State) ->
    State#state{open_reqs = OpenReqs - 1}.

-spec inc_open_reqs(State0 :: state()) -> State1 :: state().
inc_open_reqs(#state{open_reqs = OpenReqs} = State) ->
    State#state{open_reqs = OpenReqs + 1}.
