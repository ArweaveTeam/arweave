%%% @doc Serialize store registration and host lifecycle transitions.
-module(arweave_sync_runtime).
-behaviour(gen_server).
-export([start_link/0, activate/0, deactivate/0, register_store/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
activate() -> gen_server:call(?MODULE, activate, infinity).
deactivate() ->
    case whereis(?MODULE) of
        undefined -> ok;
        _ -> gen_server:call(?MODULE, deactivate, infinity)
    end.
register_store(StoreID, Writer) ->
    gen_server:call(?MODULE, {register_store, StoreID, Writer}, infinity).

init([]) ->
    {ok, undefined}.

handle_call(activate, _From, State) ->
    %% Host metric definitions and the final startup config now exist.
    %% A zero rate pauses work, not supervision: it can change at runtime.
    Result = started(
        supervisor:start_child(
            arweave_sync_sup,
            #{
                id => arweave_sync_pipeline_sup,
                start => {arweave_sync_pipeline_sup, start_link, []},
                type => supervisor,
                shutdown => infinity
            }
        )
    ),
    {reply, Result, State};
handle_call(deactivate, _From, State) ->
    stop_child(arweave_sync_pipeline_sup),
    lists:foreach(
        fun
            ({{ingest, StoreID} = ID, _, _, _}) ->
                stop_child(ID),
                arweave_sync_ingest:reset(StoreID);
            (_) ->
                ok
        end,
        supervisor:which_children(arweave_sync_sup)
    ),
    ets:delete_all_objects(arweave_sync_state),
    {reply, ok, State};
handle_call({register_store, StoreID, Writer}, _From, State) ->
    Result =
        case arweave_sync_ingest:writer(StoreID) of
            {Writer, PID} when is_pid(PID) ->
                case is_process_alive(PID) of
                    true -> ok;
                    false -> start_ingest(StoreID, Writer)
                end;
            _ ->
                start_ingest(StoreID, Writer)
        end,
    {reply, Result, State}.

handle_cast(_Message, State) -> {noreply, State}.
handle_info(_Message, State) -> {noreply, State}.

start_ingest(StoreID, Writer) ->
    ID = {ingest, StoreID},
    stop_child(ID),
    started(
        supervisor:start_child(
            arweave_sync_sup,
            #{
                id => ID,
                start => {arweave_sync_ingest, start_link, [StoreID, Writer]}
            }
        )
    ).

stop_child(ID) ->
    case supervisor:terminate_child(arweave_sync_sup, ID) of
        ok -> supervisor:delete_child(arweave_sync_sup, ID);
        {error, not_found} -> ok
    end.

started({ok, _}) -> ok;
started({error, {already_started, _}}) -> ok;
started(Error) -> Error.
