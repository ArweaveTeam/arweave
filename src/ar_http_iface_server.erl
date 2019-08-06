%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).

-export([start/3]).
-export([reregister/1, reregister/2]).
-export([split_path/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MAX_PARALLEL_HASH_LIST_REQUESTS, 1).
-define(MAX_PARALLEL_ARQL_REQUESTS, 10).
-define(MAX_PARALLEL_GATEWAY_ARQL_REQUESTS, infinity).

-define(HTTP_IFACE_MIDDLEWARES, [
	ar_blacklist_middleware,
	ar_http_body_middleware,
	cowboy_router,
	ar_http_iface_middleware,
	cowboy_handler
]).

-define(HTTP_IFACE_ROUTES, [
	{'_', [
		{"/metrics/[:registry]", prometheus_cowboy2_handler, []},
		{"/[...]", ar_http_iface_handler, []}
	]}
]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the Arweave HTTP API and returns a process ID.
start(Port, GatewayOpts, HttpNodes) ->
	reregister_from_proplist([
		http_entrypoint_node,
		http_service_node,
		http_bridge_node
	], HttpNodes),
	do_start(Port, GatewayOpts).

reregister_from_proplist(Names, HttpNodes) ->
	lists:foreach(fun(Name) ->
		reregister(Name, proplists:get_value(Name, HttpNodes))
	end, Names).

%% @doc Helper function : registers a new node as the entrypoint.
reregister(Node) ->
	reregister(http_entrypoint_node, Node).
reregister(_, undefined) -> not_registering;
reregister(Name, Node) ->
	case erlang:whereis(Name) of
		undefined -> do_nothing;
		_ -> erlang:unregister(Name)
	end,
	erlang:register(Name, Node).

split_path(Path) ->
	binary:split(Path, <<"/">>, [global, trim_all]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Start the server
do_start(Port, GatewayOpts) ->
	Dispatch = cowboy_router:compile(?HTTP_IFACE_ROUTES),
	HttpIfaceEnv = #{ dispatch => Dispatch },
	ok = ar_semaphore:start_link(hash_list_semaphore, ?MAX_PARALLEL_HASH_LIST_REQUESTS),
	ok = ar_semaphore:start_link(arql_semaphore, ?MAX_PARALLEL_ARQL_REQUESTS),
	ok = ar_semaphore:start_link(gateway_arql_semaphore, ?MAX_PARALLEL_GATEWAY_ARQL_REQUESTS),
	ok = ar_blacklist_middleware:start(),
	ok = start_http_iface_listener(Port, HttpIfaceEnv),
	ok = start_http_gateway_listener(GatewayOpts),
	ok = start_https_gateway_listener(GatewayOpts, HttpIfaceEnv),
	ok.

start_http_iface_listener(Port, HttpIfaceEnv) ->
	TransportOpts = [{port, Port}],
	ProtocolOpts = protocol_opts([{http_iface, HttpIfaceEnv}]),
	{ok, _} = cowboy:start_clear(ar_http_iface_listener, TransportOpts, ProtocolOpts),
	ok.

start_http_gateway_listener({on, Domain, CustomDomains}) ->
	TransportOpts = [{port, 80}],
	ProtocolOpts = protocol_opts([{gateway, Domain, CustomDomains}]),
	{ok, _} = cowboy:start_clear(ar_http_gateway_listener, TransportOpts, ProtocolOpts),
	ok;
start_http_gateway_listener(off) ->
	ok.

start_https_gateway_listener({on, Domain, CustomDomains}, HttpIfaceEnv) ->
	SniHosts = derive_sni_hosts(CustomDomains),
	TransportOpts = [
		{port, 443},
		{certfile, "priv/tls/cert.pem"},
		{keyfile, "priv/tls/key.pem"},
		{sni_hosts, SniHosts}
	] ++ cacertfile_when_present(),
	ProtocolOpts = protocol_opts([{http_iface, HttpIfaceEnv}, {gateway, Domain, CustomDomains}]),
	{ok, _} = cowboy:start_tls(ar_https_gateway_listener, TransportOpts, ProtocolOpts),
	ok;
start_https_gateway_listener(off, _) ->
	ok.

protocol_opts(List) ->
	Opts1 = #{ middlewares := Middlewares1, env := Env1 } =
		case proplists:lookup(http_iface, List) of
			{http_iface, HttpIfaceEnv} ->
				#{
					middlewares => ?HTTP_IFACE_MIDDLEWARES,
					env => HttpIfaceEnv,
					metrics_callback => fun prometheus_cowboy2_instrumenter:observe/1,
					stream_handlers => [cowboy_metrics_h, cowboy_stream_h]
				};
			none ->
				#{ middlewares => [], env => #{} }
		end,
	Opts2 =
		case proplists:lookup(gateway, List) of
			{gateway, Domain, CustomDomains} ->
				Opts1#{
					middlewares := [ar_gateway_middleware | Middlewares1],
					env := Env1#{
						gateway => {Domain, CustomDomains},
						arql_semaphore => gateway_arql_semaphore
					}
				};
			none ->
				Opts1#{ env := Env1#{ arql_semaphore => arql_semaphore } }
		end,
	Opts2.

derive_sni_hosts(Domains) ->
	lists:map(fun(Domain) ->
		SDomain = binary_to_list(Domain),
		{SDomain, [
			{certfile, "priv/tls/" ++ SDomain ++ "/cert.pem"},
			{keyfile, "priv/tls/" ++ SDomain ++ "/key.pem"}
		] ++ cacertfile_when_present(SDomain)}
	end, Domains).

cacertfile_when_present() ->
	cacertfile_when_present(apex_domain).

cacertfile_when_present(Domain) ->
	Filename = case Domain of
		apex_domain ->
			"priv/tls/cacert.pem";
		CustomDomain ->
			"priv/tls/" ++ CustomDomain ++ "/cacert.pem"
	end,
	case filelib:is_file(Filename) of
		true ->
			[{cacertfile, Filename}];
		false ->
			[]
	end.
