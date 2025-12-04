%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration Parameters.
%%% @end
%%%===================================================================
-module(arweave_config_parameters).
-export([init/0]).
-include("arweave_config.hrl").

%%--------------------------------------------------------------------
%% @doc returns a list of map containing arweave parameters.
%% @end
%%--------------------------------------------------------------------
init() ->
	[
		#{
			parameter_key => [data,directory],
			default => "./data",
			runtime => false,
			type => path,
			deprecated => false,
			legacy => data_dir,
			required => true,
			short_description => "",
			long_description => "",
			environment => <<"AR_DATA_DIRECTORY">>,
			short_argument => $D,
			long_argument => <<"--data-directory">>,
			handle_get => fun legacy_get/2,
			handle_set => fun legacy_set/3
		},
	 	#{
			parameter_key => [debug],
			default => false,
			runtime => true,
			type => boolean,
			deprecated => false,
			legacy => debug,
			required => false,
			short_description => "",
			long_description => "",
			environment => <<"AR_DEBUG">>,
			short_argument => $d,
			long_argument => <<"--debug">>,
			handle_get => fun legacy_get/2,
			handle_set => fun
				(K, V, S = #{ config := #{ debug := Old }}) ->
					case {V, Old} of
						{true, true} ->
							ignore;
						{false, false} ->
							ignore;
						{false, true} ->
							logger:set_application_level(arweave_config, none),
							logger:set_application_level(arweave, none),
							legacy_set(K, V, S);
						{true, false} ->
							logger:set_application_level(arweave_config, debug),
							logger:set_application_level(arweave, debug),
							legacy_set(K, V, S)
					end;
				(K, V, S) ->
					logger:set_application_level(arweave_config, debug),
					logger:set_application_level(arweave, debug),
					legacy_set(K, V, S)
			end
		},

		%-----------------------------------------------------
		% network global configuration
		%-----------------------------------------------------
		#{
			% see inet:inet_backend().
			parameter_key => [network,socket,backend],
			default => ?CONFIG_NETWORK_SOCKET_BACKEND,
			runtime => false,
			type => inet_backend,
			handle_set => fun legacy_set/3
		},

		%-----------------------------------------------------
		% arweave client (gun) global configuration
		%-----------------------------------------------------
		#{
			% see gun:http_opts(). default value is 15000.
			parameter_key => [network,client,http,closing_timeout],
			legacy => 'http_client.http.closing_timeout',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_HTTP_CLOSING_TIMEOUT,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gun:http_opts(). default to infinity.
			parameter_key => [network,client,http,keepalive],
			legacy => 'http_client.http.keepalive',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_HTTP_KEEPALIVE,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:options().
			parameter_key => [network,client,http,delay_send],
			legacy => 'http_client.tcp.delay_send',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_DELAY_SEND,
			type => boolean,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:options().
			parameter_key => [network,client,tcp,keepalive],
			legacy => 'http_client.tcp.keepalive',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_KEEPALIVE,
			type => boolean,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:options().
			parameter_key => [network,client,tcp,linger_timeout],
			legacy => 'http_client.tcp.linger_timeout',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_LINGER_TIMEOUT,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:options().
			parameter_key => [network,client,tcp,linger],
			legacy => 'http_client.tcp.linger',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_LINGER,
			type => boolean,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			%see gen_tcp:options().
			parameter_key => [network,client,tcp,nodelay],
			legacy => 'http_client.tcp.nodelay',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_NODELAY,
			type => boolean,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			%see gen_tcp:options().
			parameter_key => [network,client,tcp,send_timeout_close],
			legacy => 'http_client.tcp.send_timeout_close',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_SEND_TIMEOUT_CLOSE,
			type => boolean,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			%see gen_tcp:options().
			parameter_key => [network,client,tcp,send_timeout],
			legacy => 'http_client.tcp.send_timeout',
			environment => true,
			default => ?CONFIG_NETWORK_CLIENT_TCP_SEND_TIMEOUT,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gun:opts(). set retry value.
			parameter_key => [network,client,http,connection,retry],
			default => ?CONFIG_NETWORK_CLIENT_HTTP_CONNECTION_RETRY,
			environment => true,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gun:opts(). set connect_timeout value
			parameter_key => [network,client,http,connection,timeout],
			default => ?CONFIG_NETWORK_CLIENT_HTTP_CONNECTION_TIMEOUT,
			environment => true,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},
		#{
			% see gun:opts().
			parameter_key => [network,client,http,retry],
			default => ?CONFIG_NETWORK_CLIENT_HTTP_CONNECTION_RETRY,
			environment => true,
			type => pos_integer,
			runtime => false,
			handle_set => fun legacy_set/3
		},

		%-----------------------------------------------------
		% arweave api (cowboy) global configuration
		%-----------------------------------------------------
		#{
			% see cowboy:start_clear/3.
			parameter_key => [network,api,listen,port],
			legacy => port,
			type => tcp_port,
			default => 1984,
			runtime => false,
			environment => <<"AR_PORT">>,
			handle_set => fun legacy_set/3
		},
		#{
			% see cowboy_http:opts(). set active_n value.
			parameter_key => [network,api,http,active_n],
			legacy => 'http_api.http.active_n',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_HTTP_ACTIVE_N,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see cowboy_http:opts(). set inactivity_timeout value.
			parameter_key => [network,api,http,inactivity_timeout],
			legacy => 'http_api.http.inactivity_timeout',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_HTTP_INACTIVITY_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see cowboy_http:opts(). set linger_timeout value.
			parameter_key => [network,api,http,linger_timeout],
			legacy => 'http_api.http.linger_timeout',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_HTTP_LINGER_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see cowboy_http:opts(). set request_timeout value.
			parameter_key => [network,api,http,request_timeout],
			legacy => 'http_api.http.request_timeout',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_HTTP_REQUEST_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:listen_option().
			parameter_key => [network,api,tcp,backlog],
			legacy => 'http_api.tcp.backlog',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_BACKLOG,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,delay_send],
			legacy => 'http_api.tcp.delay_send',
			type => boolean,
			default => ?CONFIG_NETWORK_API_TCP_DELAY_SEND,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,keepalive],
			legacy => 'http_api.tcp.keepalive',
			type => boolean,
			default => ?CONFIG_NETWORK_API_TCP_KEEPALIVE,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,linger_timeout],
			legacy => 'http_api.tcp.linger_timeout',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_LINGER_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,linger],
			legacy => 'http_api.tcp.linger',
			type => boolean,
			default => ?CONFIG_NETWORK_API_TCP_LINGER,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% @todo to check.
			parameter_key => [network,api,tcp,listener_shutdown],
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_LISTENER_SHUTDOWN,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see ranch:transport_opts().
			parameter_key => [network,api,tcp,num_acceptors],
			legacy => 'http_api.tcp.num_acceptors',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_NUM_ACCEPTORS,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,send_timeout_close],
			legacy => 'http_api.tcp.send_timeout_close',
			type => boolean,
			default => ?CONFIG_NETWORK_API_TCP_SEND_TIMEOUT_CLOSE,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,send_timeout],
			legacy => 'http_api.tcp.send_timeout',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_SEND_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see ranch:transport_opts(). set max_connections value.
			parameter_key => [network,api,tcp,connections,max],
			legacy => 'http_api.tcp.max_connections',
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_CONNECTIONS_MAX,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,nodelay],
			legacy => 'http_api.tcp.nodelay',
			type => boolean,
			default => ?CONFIG_NETWORK_API_TCP_NODELAY,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},
		#{
			% see gen_tcp:option().
			parameter_key => [network,api,tcp,idle_timeout],
			legacy => http_api_transport_idle_timeout,
			type => pos_integer,
			default => ?CONFIG_NETWORK_API_TCP_IDLE_TIMEOUT,
			runtime => false,
			environment => true,
			handle_set => fun legacy_set/3
		},

		%-----------------------------------------------------
		% arweave_config http api parameters
		%-----------------------------------------------------
		#{
			parameter_key => [config,http,api,enabled],
			environment => <<"AR_CONFIG_HTTP_API_ENABLED">>,
			short_description => <<"enable arweave configuration http api interface">>,
			% @todo enable it by default after testing
			default => false,
			type => boolean,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,port],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_PORT">>,
			short_description => "set arweave configuration http api interface port",
			default => 4891,
			type => tcp_port,
			required => false,
			runtime => false
		},
		#{
			parameter_key => [config,http,api,listen,address],
			environment => <<"AR_CONFIG_HTTP_API_LISTEN_ADDRESS">>,
			short_description => "set arweave configuration http api listen address",
			type => [ipv4, file],
			required => false,
			% can be an ip address or an unix socket path,
			% the configuration should be transparent
			% though and we should avoid using
			%   {local, socket_path}
			% the rule is probably to say if the value
			% start with / then this is an unix socket,
			% else this is an ip address or an hostname.
			default => <<"127.0.0.1">>,
			runtime => false
		}
		% @todo implement read, write and token parameters
		% #{
		% 	parameter => [config,http,api,read],
		% 	environment => <<"AR_CONFIG_HTTP_API_READ">>,
		% 	short_description => "allow read (get method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% #{
		% 	parameter => [config,http,api,write],
		% 	environment => <<"AR_CONFIG_HTTP_API_WRITE">>,
		% 	short_description => "allow write (post method) on arweave configuration http api",
		% 	type => boolean,
		% 	required => false,
		% 	default => true
		% },
		% #{
		% 	parameter => [config,http,api,token],
		% 	environment => <<"AR_CONFIG_HTTP_API_TOKEN">>,
		% 	short_description => "set an access token for arweave configuration http api interface",
		% 	type => string,
		% 	required => false,
		% 	default => <<>>
		% }
	].

legacy_get(_K, #{ spec := #{ legacy := L }}) ->
	V = arweave_config_legacy:get(L),
	{ok, V};
legacy_get(_K, _) ->
	{error, not_found}.

legacy_set(_K, V, #{ spec := #{ legacy := L }}) ->
	arweave_config_legacy:set(L, V),
	{store, V};
legacy_set(_K, V, _) ->
	{store, V}.
