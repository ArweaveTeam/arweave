%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).

-export([start/1]).
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
	ar_arql_middleware,
	ar_http_iface_middleware,
	cowboy_handler
]).

-define(HTTP_IFACE_ROUTES, [
	{"/metrics/[:registry]", prometheus_cowboy2_handler, []},
	{"/[...]", ar_http_iface_handler, []}
]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the Arweave HTTP API and returns a process ID.
start(Opts) ->
	reregister_from_proplist([
		http_entrypoint_node,
		http_service_node,
		http_bridge_node
	], Opts),
	do_start(Opts).

reregister_from_proplist(Names, Opts) ->
	lists:foreach(fun(Name) ->
		reregister(Name, proplists:get_value(Name, Opts))
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
do_start(Opts) ->
	ok = ar_semaphore:start_link(hash_list_semaphore, ?MAX_PARALLEL_HASH_LIST_REQUESTS),
	ok = ar_semaphore:start_link(arql_semaphore, ?MAX_PARALLEL_ARQL_REQUESTS),
	ok = ar_semaphore:start_link(gateway_arql_semaphore, ?MAX_PARALLEL_GATEWAY_ARQL_REQUESTS),
	ok = ar_blacklist_middleware:start(),
	ok = start_http_iface_listener(Opts),
	ok = start_gateway_listeners(Opts),
	ok.

start_http_iface_listener(Opts) ->
	Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
	HttpIfaceEnv = #{ dispatch => Dispatch },
	TransportOpts = #{
		max_connections => proplists:get_value(max_connections, Opts),
		socket_opts => [{port, proplists:get_value(port, Opts)}]
	},
	ProtocolOpts = protocol_opts([{http_iface, HttpIfaceEnv}]),
	{ok, _} = cowboy:start_clear(ar_http_iface_listener, TransportOpts, ProtocolOpts),
	ok.

start_gateway_listeners(Opts) ->
	case proplists:get_value(gateway_domain, Opts) of
		Domain when is_binary(Domain) ->
			ok = start_http_gateway_listener(Opts),
			ok = start_https_gateway_listener(Opts),
			ok;
		not_set ->
			ok
	end.

start_http_gateway_listener(Opts) ->
	Domain = proplists:get_value(gateway_domain, Opts),
	CustomDomains = proplists:get_value(gateway_custom_domains, Opts),
	TransportOpts = #{
		max_connections => 10,
		socket_opts => [{port, 80}]
	},
	ProtocolOpts = protocol_opts([{gateway, Domain, CustomDomains}]),
	{ok, _} = cowboy:start_clear(ar_http_gateway_listener, TransportOpts, ProtocolOpts),
	ok.

start_https_gateway_listener(Opts) ->
	PrivDir = code:priv_dir(arweave),
	Domain = proplists:get_value(gateway_domain, Opts),
	CustomDomains = proplists:get_value(gateway_custom_domains, Opts),
	Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
	HttpIfaceEnv = #{ dispatch => Dispatch },
	SniHosts = derive_sni_hosts(CustomDomains),
	TransportOpts = #{
		max_connections => proplists:get_value(max_gateway_connections, Opts),
		socket_opts => [
			{port, 443},
			{certfile, filename:join([PrivDir, "tls/cert.pem"])},
			{keyfile, filename:join([PrivDir, "tls/key.pem"])},
			{sni_hosts, SniHosts}
		] ++ cacertfile_when_present()
	},
	ProtocolOpts = protocol_opts([{http_iface, HttpIfaceEnv}, {gateway, Domain, CustomDomains}]),
	{ok, _} = cowboy:start_tls(ar_https_gateway_listener, TransportOpts, ProtocolOpts),
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
				Opts1#{
					env := Env1#{ arql_semaphore => arql_semaphore },
					metrics_callback => fun collect_http_response_metrics/1
				}
		end,
	Opts2.

derive_sni_hosts(Domains) ->
	PrivDir = code:priv_dir(arweave),
	lists:map(fun(Domain) ->
		SDomain = binary_to_list(Domain),
		{SDomain, [
			{certfile, filename:join([PrivDir, "tls", SDomain, "cert.pem"])},
			{keyfile, filename:join([PrivDir, "tls", SDomain, "key.pem"])}
		] ++ cacertfile_when_present(SDomain)}
	end, Domains).

cacertfile_when_present() ->
	cacertfile_when_present(apex_domain).

cacertfile_when_present(Domain) ->
	PrivDir = code:priv_dir(arweave),
	Filename = case Domain of
		apex_domain ->
			filename:join([PrivDir, "tls/cacert.pem"]);
		CustomDomain ->
			filename:join([PrivDir, "tls", CustomDomain, "cacert.pem"])
	end,
	case filelib:is_file(Filename) of
		true ->
			[{cacertfile, Filename}];
		false ->
			[]
	end.

collect_http_response_metrics(Metrics) ->
	Req = maps:get(req, Metrics),
	prometheus_counter:inc(
		http_server_served_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		maps:get(resp_body_length, Metrics, 0)
	),
	prometheus_cowboy2_instrumenter:observe(Metrics).
