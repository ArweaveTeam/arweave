%%% ----------------------------------------------------------------------------
%%% @copyright (C) 2013, Erlang Solutions Ltd
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc Fusco Client Pool
%%%
%%% Pool of clients connected to the same server. Clients do not share state
%%% Recommended for BOSH where connections do not share cookies or any other
%%% headers state
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco_cp).

-behaviour(gen_server).

%% API
-export([start/3,
         start_link/3,
         get_client/1,
         free_client/2,
         stop/1
        ]).

-export([request/7]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {destination,
                options,
                max,
                total,
                free,
                busy,
                queue
               }).

%%%===================================================================
%%% API
%%%===================================================================
start(Destination, Options, MaxClients) ->
    verify_max(MaxClients),
    gen_server:start(?MODULE, [Destination, Options, MaxClients], []).

start_link(Destination, Options, MaxClients) ->
    verify_max(MaxClients),
    gen_server:start_link(?MODULE, [Destination, Options, MaxClients], []).

get_client(Pool) ->
    try
        gen_server:call(Pool, get_client)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

free_client(Pool, Client) ->
    gen_server:cast(Pool, {free_client, Client}).

stop(Pool) ->
    gen_server:cast(Pool, stop).

request(Pool, Path, Method, Hdrs, Body, SendRetry, Timeout) ->
    case get_client(Pool) of
        {error, _} = Error ->
            Error;
        Client ->
            Reply = fusco:request(Client, Path, Method, Hdrs, Body, SendRetry,
                                  Timeout),
            free_client(Pool, Client),
            Reply
    end.
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Destination, Options, MaxClients]) ->
    process_flag(trap_exit, true),
    {ok, #state{destination = Destination,
                options = Options,
                total = 0,
                max = MaxClients,
                free = [],
                busy = [],
                queue = queue:new()
               }, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_client, _From, State = #state{free = [Client | Free],
                                              busy = Busy}) ->
    {reply, Client, State#state{free = Free,
                                busy = [Client | Busy]}};
handle_call(get_client, _From, State = #state{destination = Destination,
                                              options = Options,
                                              free = [],
                                              max = M,
                                              total = T,
                                              busy = Busy})
  when M > T ->
    {ok, Pid} = fusco:start_link(Destination, Options),
    {reply, Pid, State#state{total = T + 1,
                             busy = [Pid | Busy]}};
handle_call(get_client, From, State = #state{free = [],
                                             max = M,
                                             total = T,
                                             queue = Queue})
  when M == T ->
    {noreply, State#state{queue = queue:in(From, Queue)}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({free_client, Pid}, State = #state{free = Free,
                                                      busy = Busy,
                                                      queue = Queue}) ->
    case queue:is_empty(Queue) of
        true ->
            {noreply, State#state{free = [Pid | Free],
                                    busy = lists:delete(Pid, Busy)}};
        false ->
            {{value, From}, Q2} = queue:out(Queue),
            gen_server:reply(From, Pid),
            {noreply, State#state{queue = Q2}}
    end;
handle_cast(stop, State) ->
    {stop, normal, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', From, _Reason}, State = #state{free = Free,
                                                    busy = Busy,
                                                    total = Total}) ->
    {noreply, State#state{free = lists:delete(From, Free),
                          busy = lists:delete(From, Busy),
                          total = Total - 1}};
handle_info(timeout, #state{free = [], busy = [],
                            destination = Destination,
                            options = Options} = State) ->
    {ok, Pid} = fusco:start_link(Destination, Options),
    {noreply, State#state{free = [Pid], total = 1}};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{free = Free, busy = Busy}) ->
    [fusco:disconnect(F) || F <- Free],
    [fusco:disconnect(B) || B <- Busy],
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
verify_max(Integer) when is_integer(Integer), Integer > 0 ->
    ok;
verify_max(_) ->
    throw(invalid_parameter).
