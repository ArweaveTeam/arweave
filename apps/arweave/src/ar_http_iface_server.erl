%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).
-behavior(gen_server).

-export([start_link/0]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([split_path/1, label_http_path/1, label_req/1]).
-export([set_max_connections/1, set_protocol_opt/2]).

-define(HTTP_IFACE_LISTENER, ar_http_iface_listener).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HTTP_IFACE_MIDDLEWARES, [
                                 ar_http_iface_rate_limiter_middleware,
                                 ar_network_middleware,
                                 cowboy_router,
                                 ar_http_iface_middleware,
                                 cowboy_handler
                                ]).

-define(HTTP_IFACE_ROUTES, [
	{"/metrics/[:registry]", arweave_metrics_cowboy_handler, []},
	{"/[...]", ar_http_iface_handler, []}
]).

-define(ENDPOINTS, ["info", "block", "block_announcement", "block2", "tx", "tx2",
                    "queue", "recent_hash_list", "recent_hash_list_diff", "tx_anchor", "arql", "time",
                    "chunk", "chunk2", "data_sync_record", "sync_buckets", "footprint_buckets",
                    "wallet", "unsigned_tx",
                    "peers", "hash_list", "block_index", "block_index2", "total_supply", "wallet_list",
                    "height", "metrics", "vdf", "vdf2", "partial_solution", "pool_cm_jobs"]).

%%%===================================================================
%%% Public interface.
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    ?LOG_INFO([{start, ?MODULE}, {pid, self()}]),
                                                % this process needs to be stopped in a clean way,
                                                % if something goes wrong, the connections must
                                                % be cleaned before leaving.
    erlang:process_flag(trap_exit, true),
    case start_http_iface_listener() of
        {ok, Pid} -> {ok, Pid};
        Else -> {error, Else}
    end.

split_path(Path) ->
    binary:split(Path, <<"/">>, [global, trim_all]).

%% @doc Return the HTTP path label,
%% Used for cowboy_requests_total and gun_requests_total metrics, as well as P3 handling.
label_http_path(Path) when is_list(Path) ->
    name_route(Path);
label_http_path(Path) ->
    label_http_path(split_path(Path)).

label_req(Req) ->
    SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
    ar_http_iface_server:label_http_path(SplitPath).

handle_call(Msg, From, State) ->
    ?LOG_WARNING([{process, ?MODULE}, {received, Msg}, {from, From}]),
    {noreply, State}.

handle_cast(apply_protocol_opts, State) ->
    case listener_started() of
        false -> ok;
        true -> ranch:set_protocol_options(?HTTP_IFACE_LISTENER, build_protocol_opts())
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    ?LOG_WARNING([{process, ?MODULE}, {received, Msg}]),
    {noreply, State}.

handle_info(Msg = {'EXIT', _From, Reason}, _State) ->
    ?LOG_ERROR([{process, ?MODULE}, {received, Msg}]),
    {stop, Reason};
handle_info(Msg, State) ->
    ?LOG_WARNING([{process, ?MODULE}, {received, Msg}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
    ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
start_http_iface_listener() ->
    cowboy:start_clear(
      ?HTTP_IFACE_LISTENER, build_transport_opts(), build_protocol_opts()).

%% @doc Build the ranch transport options map from the current config. Read once
%% at listener start — the transport socket opts are not runtime-updated.
build_transport_opts() ->
    Backlog = arweave_config:get([network, server, tcp, backlog]),
    DelaySend = arweave_config:get([network, server, tcp, delay_send]),
    Keepalive = arweave_config:get([network, server, tcp, keepalive]),
    Linger = arweave_config:get([network, server, tcp, linger]),
    LingerTimeout = arweave_config:get([network, server, tcp, linger_timeout]),
    MaxConnections = arweave_config:get([network, server, tcp, max_connections]),
    Nodelay = arweave_config:get([network, server, tcp, nodelay]),
    NumAcceptors = arweave_config:get([network, server, tcp, num_acceptors]),
    SendTimeoutClose = arweave_config:get([network, server, tcp, send_timeout_close]),
    SendTimeout = arweave_config:get([network, server, tcp, send_timeout]),
    ListenerShutdown = arweave_config:get([network, server, tcp, listener_shutdown]),
    Port = arweave_config:get([port]),
    #{
                                                % ranch_tcp parameters
      backlog => Backlog,
      delay_send => DelaySend,
      keepalive => Keepalive,
      linger => {Linger, LingerTimeout},
      max_connections => MaxConnections,
      nodelay => Nodelay,
      num_acceptors => NumAcceptors,
      send_timeout_close => SendTimeoutClose,
      send_timeout => SendTimeout,
      shutdown => ListenerShutdown,
      socket_opts => [
                      {port, Port}
                     ]
     }.

%% @doc Build the cowboy protocol options map from the current config. Shared by
%% listener startup and the runtime protocol-opt updater.
build_protocol_opts() ->
    Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
    ActiveN = arweave_config:get([network, server, http, active_n]),
    InactivityTimeout = arweave_config:get([network, server, http, inactivity_timeout]),
    HTTPLingerTimeout = arweave_config:get([network, server, http, linger_timeout]),
    RequestTimeout = arweave_config:get([network, server, http, request_timeout]),
    IdleTimeout = arweave_config:get([network, server, transport, idle_timeout]),
    #{
      active_n => ActiveN,
      inactivity_timeout => InactivityTimeout,
      linger_timeout => HTTPLingerTimeout,
      request_timeout => RequestTimeout,
      idle_timeout => IdleTimeout,
      middlewares => ?HTTP_IFACE_MIDDLEWARES,
      env => #{
               dispatch => Dispatch
              },
      metrics_callback => fun prometheus_cowboy2_instrumenter:observe/1,
      stream_handlers => [cowboy_metrics_h, cowboy_stream_h]
     }.

%% @doc Set the listener's max connection count at runtime (load-safe).
set_max_connections(V) ->
    case listener_started() of
        false -> ok;
        true -> ranch:set_max_connections(?HTTP_IFACE_LISTENER, V)
    end,
    ok.

%% @doc Rebuild the cowboy protocol options from config and apply them to the
%% running listener (load-safe). Applies to new connections. Casts to the server
%% so the rebuild runs after the registry has stored the new value: the
%% handle_set that calls this runs before the store, so reading config
%% synchronously here would still see the old value.
set_protocol_opt(_Key, _V) ->
    gen_server:cast(?MODULE, apply_protocol_opts),
    ok.

%% @doc Whether the cowboy/ranch listener is up. Setters are no-ops until then.
listener_started() ->
    try
        _ = ranch:info(?HTTP_IFACE_LISTENER),
        true
    catch
        _:_ -> false
    end.

name_route([]) ->
    "/";
name_route([<<"current_block">>]) ->
    "/current/block";
name_route([<<_Hash:43/binary, _MaybeExt/binary>>]) ->
    "/{hash}[.{ext}]";
name_route([Bin]) ->
    L = binary_to_list(Bin),
    case lists:member(L, ?ENDPOINTS) of
        true ->
            "/" ++ L;
        false ->
            undefined
    end;
name_route([<<"peer">> | _]) ->
    "/peer/...";

name_route([<<"jobs">>, _PrevOutput]) ->
    "/jobs/{prev_output}";

name_route([<<"vdf">>, <<"session">>]) ->
    "/vdf/session";
name_route([<<"vdf2">>, <<"session">>]) ->
    "/vdf2/session";
name_route([<<"vdf3">>, <<"session">>]) ->
    "/vdf3/session";
name_route([<<"vdf4">>, <<"session">>]) ->
    "/vdf4/session";

name_route([<<"vdf">>, <<"previous_session">>]) ->
    "/vdf/previous_session";
name_route([<<"vdf2">>, <<"previous_session">>]) ->
    "/vdf2/previous_session";
name_route([<<"vdf4">>, <<"previous_session">>]) ->
    "/vdf4/previous_session";

name_route([<<"tx">>, <<"pending">>]) ->
    "/tx/pending";
name_route([<<"tx">>, _Hash, <<"status">>]) ->
    "/tx/{hash}/status";
name_route([<<"tx">>, _Hash]) ->
    "/tx/{hash}";
name_route([<<"tx2">>, _Hash]) ->
    "/tx2/{hash}";
name_route([<<"unconfirmed_tx">>, _Hash]) ->
    "/unconfirmed_tx/{hash}";
name_route([<<"unconfirmed_tx2">>, _Hash]) ->
    "/unconfirmed_tx2/{hash}";
name_route([<<"tx">>, _Hash, << "data" >>]) ->
    "/tx/{hash}/data";
name_route([<<"tx">>, _Hash, << "data.", _/binary >>]) ->
    "/tx/{hash}/data.{ext}";
name_route([<<"tx">>, _Hash, << "offset" >>]) ->
    "/tx/{hash}/offset";
name_route([<<"tx">>, _Hash, _Field]) ->
    "/tx/{hash}/{field}";

name_route([<<"chunk">>, _Offset]) ->
    "/chunk/{offset}";
name_route([<<"chunk2">>, _Offset]) ->
    "/chunk2/{offset}";

name_route([<<"unconfirmed_chunk">>, _EncodedTXID, _Offset]) ->
    "/unconfirmed_chunk/{txid}/{offset}";

name_route([<<"data_roots">>, _Offset]) ->
    "/data_roots/{offset}";

name_route([<<"chunk_proof">>, _Offset]) ->
    "/chunk_proof/{offset}";
name_route([<<"chunk_proof2">>, _Offset]) ->
    "/chunk_proof2/{offset}";

name_route([<<"data_sync_record">>, _Start, _Limit]) ->
    "/data_sync_record/{start}/{limit}";
name_route([<<"data_sync_record">>, _Start, _End, _Limit]) ->
    "/data_sync_record/{start}/{end}/{limit}";

name_route([<<"footprints">>, _Partition, _Number]) ->
    "/footprints/{partition}/{footprint_number}";

name_route([<<"price">>, _SizeInBytes]) ->
    "/price/{bytes}";
name_route([<<"price">>, _SizeInBytes, _Addr]) ->
    "/price/{bytes}/{address}";

name_route([<<"price2">>, _SizeInBytes]) ->
    "/price2/{bytes}";
name_route([<<"price2">>, _SizeInBytes, _Addr]) ->
    "/price2/{bytes}/{address}";

name_route([<<"v2price">>, _SizeInBytes]) ->
    "/v2price/{bytes}";
name_route([<<"v2price">>, _SizeInBytes, _Addr]) ->
    "/v2price/{bytes}/{address}";

name_route([<<"optimistic_price">>, _SizeInBytes]) ->
    "/optimistic_price/{bytes}";
name_route([<<"optimistic_price">>, _SizeInBytes, _Addr]) ->
    "/optimistic_price/{bytes}/{address}";

name_route([<<"reward_history">>, _BH]) ->
    "/reward_history/{block_hash}";

name_route([<<"block_time_history">>, _BH]) ->
    "/block_time_history/{block_hash}";

name_route([<<"wallet">>, _Addr, <<"balance">>]) ->
    "/wallet/{addr}/balance";
name_route([<<"wallet">>, _Addr, <<"last_tx">>]) ->
    "/wallet/{addr}/last_tx";
name_route([<<"wallet">>, _Addr, <<"txs">>]) ->
    "/wallet/{addr}/txs";
name_route([<<"wallet">>, _Addr, <<"txs">>, _EarliestTX]) ->
    "/wallet/{addr}/txs/{earliest_tx}";
name_route([<<"wallet">>, _Addr, <<"deposits">>]) ->
    "/wallet/{addr}/deposits";
name_route([<<"wallet">>, _Addr, <<"deposits">>, _EarliestDeposit]) ->
    "/wallet/{addr}/deposits/{earliest_deposit}";

name_route([<<"wallet_list">>, _Root]) ->
    "/wallet_list/{root_hash}";
name_route([<<"wallet_list">>, _Root, _Cursor]) ->
    "/wallet_list/{root_hash}/{cursor}";
name_route([<<"wallet_list">>, _Root, _Addr, <<"balance">>]) ->
    "/wallet_list/{root_hash}/{addr}/balance";

name_route([<<"block_index">>, _From, _To]) ->
    "/block_index/{from}/{to}";
name_route([<<"block_index2">>, _From, _To]) ->
    "/block_index2/{from}/{to}";
name_route([<<"hash_list">>, _From, _To]) ->
    "/hash_list/{from}/{to}";
name_route([<<"hash_list2">>, _From, _To]) ->
    "/hash_list2/{from}/{to}";

name_route([<<"block">>, <<"hash">>, _IndepHash]) ->
    "/block/hash/{indep_hash}";
name_route([<<"block">>, <<"height">>, _Height]) ->
    "/block/height/{height}";
name_route([<<"block2">>, <<"hash">>, _IndepHash]) ->
    "/block2/hash/{indep_hash}";
name_route([<<"block2">>, <<"height">>, _Height]) ->
    "/block2/height/{height}";
name_route([<<"block">>, _Type, _IDBin, _Field]) ->
    "/block/{type}/{id_bin}/{field}";
name_route([<<"block">>, <<"height">>, _Height, <<"wallet">>, _Addr, <<"balance">>]) ->
    "/block/height/{height}/wallet/{addr}/balance";
name_route([<<"block">>, <<"current">>]) ->
    "/block/current";

name_route([<<"coordinated_mining">>, <<"h1">>]) ->
    "/coordinated_mining/h1";
name_route([<<"coordinated_mining">>, <<"h2">>]) ->
    "/coordinated_mining/h2";
name_route([<<"coordinated_mining">>, <<"partition_table">>]) ->
    "/coordinated_mining/partition_table";
name_route([<<"coordinated_mining">>, <<"publish">>]) ->
    "/coordinated_mining/publish";
name_route([<<"coordinated_mining">>, <<"state">>]) ->
    "/coordinated_mining/state";


name_route(_) ->
    undefined.
