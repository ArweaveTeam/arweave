-module(ar_gateway_server).
-export([start/3]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server that will handle gateway requests.
start(Port, Domain, CustomDomains) ->
	ProtocolOpts = #{
		middlewares => [cowboy_handler],
		env => #{
			handler => ar_gateway_handler,
			handler_opts => {Domain, CustomDomains}
		}
	},
	SniHosts = derive_sni_hosts(CustomDomains),
	{ok, _} = cowboy:start_tls(ar_gateway_listener, [
		{port, Port},
		{certfile, "priv/tls/cert.pem"},
		{keyfile, "priv/tls/key.pem"},
		{sni_hosts, SniHosts}
	], ProtocolOpts).

%%%===================================================================
%%% Private functions.
%%%===================================================================

derive_sni_hosts(Domains) ->
	lists:map(fun(Domain) ->
		SDomain = binary_to_list(Domain),
		{SDomain, [
			{certfile, "priv/tls/" ++ SDomain ++ "/cert.pem"},
			{keyfile, "priv/tls/" ++ SDomain ++ "/key.pem"}
		]}
	end, Domains).
