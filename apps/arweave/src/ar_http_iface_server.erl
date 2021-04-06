%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).

-export([start/0, stop/0]).
-export([split_path/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HTTP_IFACE_MIDDLEWARES, [
	ar_blacklist_middleware,
	ar_network_middleware,
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

split_path(Path) ->
	binary:split(Path, <<"/">>, [global, trim_all]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Start the server
start() ->
	{ok, Config} = application:get_env(arweave, config),
	Semaphores = Config#config.semaphores,
	maps:map(
		fun(Name, N) ->
			ok = ar_semaphore:start_link(Name, N)
		end,
		Semaphores
	),
	ok = ar_blacklist_middleware:start(),
	ok = start_http_iface_listener(Config),
	ok = start_gateway_listeners(Config),
	ok.

start_http_iface_listener(Config) ->
	Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
	HttpIfaceEnv = #{ dispatch => Dispatch },
	TransportOpts = #{
		max_connections => Config#config.max_connections,
		socket_opts => [{port, Config#config.port}]
	},
	ProtocolOpts = protocol_opts([{http_iface, HttpIfaceEnv}]),
	{ok, _} = cowboy:start_clear(ar_http_iface_listener, TransportOpts, ProtocolOpts),
	ok.

stop() ->
	cowboy:stop_listener(ar_http_iface_listener),
	cowboy:stop_listener(ar_http_gateway_listener),
	cowboy:stop_listener(ar_https_gateway_listener).

start_gateway_listeners(Config) ->
	case Config#config.gateway_domain of
		Domain when is_binary(Domain) ->
			ok = start_http_gateway_listener(Config),
			ok = start_https_gateway_listener(Config),
			ok;
		not_set ->
			ok
	end.

start_http_gateway_listener(Config) ->
	Domain = Config#config.gateway_domain,
	CustomDomains = Config#config.gateway_custom_domains,
	TransportOpts = #{
		max_connections => 100,
		num_acceptors => 100,
		socket_opts => [{port, 80}]
	},
	ProtocolOpts = protocol_opts([{gateway, Domain, CustomDomains}]),
	{ok, _} = cowboy:start_clear(ar_http_gateway_listener, TransportOpts, ProtocolOpts),
	ok.

start_https_gateway_listener(Config) ->
	PrivDir = code:priv_dir(arweave),
	Domain = Config#config.gateway_domain,
	CustomDomains = Config#config.gateway_custom_domains,
	Dispatch = cowboy_router:compile([{'_', ?HTTP_IFACE_ROUTES}]),
	HttpIfaceEnv = #{ dispatch => Dispatch },
	SniHosts = derive_sni_hosts(CustomDomains),
	TransportOpts = #{
		max_connections => Config#config.max_gateway_connections,
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
						arql_semaphore => gateway_arql
					}
				};
			none ->
				Opts1#{
					env := Env1#{ arql_semaphore => arql },
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
	case maps:get(req, Metrics, no_req) of
		no_req ->
			ok;
		Req ->
			prometheus_counter:inc(
				http_server_served_bytes_total,
				[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
				maps:get(resp_body_length, Metrics, 0)
			),
			ResponseStatus = maps:get(resp_status, Metrics),
			%% Convert the 208 response status binary back to the integer
			%% so that it is picked up as a successful response by the cowboy
			%% instrumenter. See handle208/1 in ar_http_iface_middleware for the
			%% explanation of why 208 is converted into a binary.
			prometheus_cowboy2_instrumenter:observe(
				Metrics#{ resp_status => convert_208(ResponseStatus) }
			)
	end.

convert_208(<<"208 Already Reported">>) -> 208;
convert_208(Status) -> Status.
