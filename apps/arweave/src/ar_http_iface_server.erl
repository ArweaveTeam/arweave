%%%===================================================================
%%% @doc Handle http requests.
%%%===================================================================

-module(ar_http_iface_server).

-export([start/0, stop/0]).
-export([split_path/1, label_http_path/1]).

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
	{"/metrics/[:registry]", ar_prometheus_cowboy_handler, []},
	{"/[...]", ar_http_iface_handler, []}
]).

-define(ENDPOINTS, ["info", "block", "block_announcement", "block2", "tx", "tx2",
		"queue", "recent_hash_list", "recent_hash_list_diff", "tx_anchor", "arql", "time",
		"chunk", "chunk2", "data_sync_record", "sync_buckets", "wallet", "unsigned_tx",
		"peers", "hash_list", "block_index", "block_index2", "total_supply", "wallet_list",
		"height", "metrics", "rates"]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

split_path(Path) ->
	binary:split(Path, <<"/">>, [global, trim_all]).

%% @doc Return the HTTP path label,
%% Used for cowboy_requests_total and gun_requests_total metrics, as well as P3 handling.
label_http_path(Path) when is_list(Path) ->
	name_route(Path);
label_http_path(Path) ->
	label_http_path(split_path(Path)).

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
		socket_opts => [
                  {port, Config#config.port},
                  {keepalive, true}
                ]
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
					inactivity_timeout => 120000,
					idle_timeout => 30000,
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

name_route([]) ->
	"/";
name_route([<<"vdf">>]) ->
	"/vdf";
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

name_route([<<"data_sync_record">>, _Start, _Limit]) ->
	"/data_sync_record/{start}/{limit}";

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

name_route([<<"balance">>, _Addr, _Network, _Token]) ->
	"/balance/{address}/{network}/{token}";
name_route([<<"rates">>]) ->
	"/rates";

name_route(_) ->
	undefined.
