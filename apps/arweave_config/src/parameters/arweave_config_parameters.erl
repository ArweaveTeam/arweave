%%%===================================================================
%%% @doc Module regrouping all parameters in one place, as map.
%%% @end
%%%===================================================================
-module(arweave_config_parameters).
-export([init/0]).

%%--------------------------------------------------------------------
%% @doc parameters specification from maps.
%% @end
%%--------------------------------------------------------------------
init() ->
	[
		%---------------------------------------------------------------------
		% global configuration
		%---------------------------------------------------------------------
		#{
			configuration_key => [global, init],
			default => false,
			short_argument => undefined,
			long_argument => <<"init">>,
			legacy => init,
			runtime => false,
			short_description => [
				<<"Start a new weave.">>
			]
		},

		%---------------------------------------------------------------------
		% data configuration
		%---------------------------------------------------------------------
		#{
			configuration_key => [global,data,jobs,sync],
			legacy => sync_jobs,
			runtime => true,
			short_description => ""
		},
		% #config.header_sync_jobs
		#{
			configuration_key => [global,data,jobs,headers,sync],
			legacy => header_sync_jobs,
			runtime => true,
			short_description => ""
		},

		%---------------------------------------------------------------------
		% webui configuration
		%---------------------------------------------------------------------
	  	#{
			configuration_key => [webui, enabled],
			default => false,
			short_argument => undefined,
			long_argument => undefined,
			runtime => true,
			short_description =>
				<<"Configure web user interface">>,
			environment => <<"AR_WEBUI">>,
			type => boolean,
			handle_set =>
				fun
					(_,true,true) ->
						{ok, true};
					(_,false,false) ->
						{ok, false};
					(_,true,false) ->
						% start webui
						arweave_config_http_server:start_as_child(),
						{store, true};
					(_,false,true) ->
						% stop webui
						arweave_config_http_server:stop_as_child(),
						{store, false}
				end
		},
		#{
			configuration_key => [webui,listen,port],
			short_argument => undefined,
			long_argument => [webui,listen,port],
			runtime => true,
			short_description => [],
			long_description => [],
			type => tcp_port,
			environment => <<"AR_WEBUI_LISTEN_PORT">>
		},
		#{
			configuration_key => [webui,listen,address],
			short_argument => undefined,
			long_argument => [webui,listen,address],
			runtime => true,
			short_description => [],
			long_description => [],
			type => [ipv4,ipv6,unix_sock],
			environment => <<"AR_WEBUI_LISTEN_ADDRESS">>
		},

		%---------------------------------------------------------------------
		% chunks configuration
		%---------------------------------------------------------------------
		#{
			configuration_key => [global,chunks,verify,samples],
			runtime => false,
			legacy => verify_samples
		},
		#{
			configuration_key => [global,chunks,verify],
			runtime => false,
			legacy => verify
		},

		%---------------------------------------------------------------------
		% general network configuration
		%---------------------------------------------------------------------
		#{
			configuration_key => [global,network,connections,max],
			runtime => true,
			legacy => 'max_connections',
			type => pos_integer
		},
		#{
			configuration_key => [global,network,socket,backend],
			type => atom,
			legacy => 'network.socket.backend',
			runtime => true
		},
		#{
			configuration_key => [global,network,tcp,shutdown,mode],
			type => atom,
			legacy => 'network.tcp.shutdown.mode',
			runtime => true
		},

		%---------------------------------------------------------------------
		% http api configuration (cowboy)
		%---------------------------------------------------------------------
		#{
			configuration_key => [global,network,api,port],
			runtime => false,
			legacy => 'port',
			type => pos_integer
		},
		#{
			configuration_key => [global,network,api,tcp,max_connections],
			runtime => true,
			legacy => 'http_api.tcp.max_connections',
			type => pos_integer
		},
		#{
			configuration_key => [global,network,api,tcp,send_timeout_close],
			legacy => 'http_client.tcp.send_timeout_close',
			runtime => true,
			type => boolean
		},
		#{
			configuration_key => [global,network,api,tcp,send_timeout],
			runtime => true,
			legacy => 'http_api.tcp.send_timeout',
			type => pos_integer
		},
		#{
			configuration_key => [global,network,api,tcp,num_acceptors],
			legacy => 'http_api.tcp.num_acceptors',
			type => pos_integer,
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,nodelay],
			type => boolean,
			legacy => 'http_api.tcp.nodelay',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,listener_shutdown],
			type => atom,
			legacy => 'http_api.tcp.listener_shutdown',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,linger],
			type => boolean,
			legacy => 'http_api.tcp.linger',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,linger_timeout],
			type => pos_integer,
			legacy => 'http_api.tcp.linger_timeout',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,keepalive],
			type => pos_integer,
			legacy => 'http_api.tcp.keepalive',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,idle_timeout_seconds],
			type => pos_integer,
			legacy => 'http_api.tcp.idle_timeout_seconds',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,backlog],
			type => pos_integer,
			legacy => 'http_api.tcp.backlog',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,tcp,delay_send],
			type => boolean,
			legacy => 'http_api.tcp.delay_send',
			runtime => true
		},
		#{
			configuration_key => [global,network,api,http,request_timeout],
			type => pos_integer,
			runtime => true,
			legacy => 'http_api.http.request_timeout'
		},
		#{
			configuration_key => [global,network,api,http,linger_timeout],
			type => pos_integer,
			runtime => true,
			legacy => 'http_api.http.linger_timeout'
		},
		#{
			configuration_key => [global,network,api,http,inactivity_timeout],
			type => pos_integer,
			runtime => true,
			legacy => 'http_api.http.inactivity_timeout'
		},
		#{
			configuration_key => [global,network,api,http,active_n],
			type => pos_integer,
			runtime => true,
			legacy => 'http_api.http.active_n'
		},

		%---------------------------------------------------------------------
		% http client configuration (gun)
		%---------------------------------------------------------------------
		#{
			configuration_key => [global,network,client,tcp,nodelay],
			runtime => true,
			legacy => 'http_client.tcp.nodelay'
		},
		#{
			configuration_key => [global,network,client,tcp,send_timeout_close],
			runtime => true,
			legacy => 'http_client.tcp.send_timeout_close'
		},
		#{
			configuration_key => [global,network,client,tcp,send_timeout],
			legacy => 'http_client.tcp.send_timeout',
			runtime => true
		},
		#{
			configuration_key => [global,network,client,tcp,linger_timeout],
			legacy => 'http_client.tcp.linger_timeout',
			runtime => true
		},
		#{
			configuration_key => [global,network,client,tcp,linger],
			legacy => 'http_client.tcp.linger',
			runtime => true
		},
		#{
			configuration_key => [global,network,client,tcp,keepalive],
			legacy => 'http_client.tcp.keepalive',
			runtime => true,
			type => pos_integer
		},
		#{
			configuration_key => [global,network,client,tcp,delay_send],
			legacy => 'http_client.tcp.delay_send',
			runtime => true,
			type => boolean
		},
		#{
			configuration_key => [global,network,client,http,keepalive],
			legacy => 'http_client.http.keepalive',
			runtime => true,
			type => pos_integer
		},
		#{
			configuration_key => [global,network,client,http,closing_timeout],
			legacy => 'http_client.http.closing_timeout',
			runtime => true,
			type => pos_integer
		},

		%---------------------------------------------------------------------
		% mining configuration
		%---------------------------------------------------------------------
		#{
			configuration_key => [mining,difficulty],
			legacy => 'diff',
			runtime => false,
			type => pos_integer
		},

		%---------------------------------------------------------------------
		% blocks configuration
		%---------------------------------------------------------------------

		%---------------------------------------------------------------------
		% chunks configuration
		%---------------------------------------------------------------------

		%---------------------------------------------------------------------
		% transactions
		%---------------------------------------------------------------------
		% [transactions, whitelist, {txid}] => #{},
		% [transactions, blacklist, {txid}] => #{},
		% [transactions, urls, blacklist, {url}] => #{},
		% [transactions, urls, whitelist, {url}] => #{},

		%---------------------------------------------------------------------
		% peer configuration
		%---------------------------------------------------------------------
		% [peers, verify] => #{ legacy => [verify] },
		% [peers,{peer},block_gossip_peers] => #{},
		% [peers,{peer},trusted] => #{},
		% [peers,{peer},vdf,server] => #{},
		% [peers,{peer},vdf,client] => #{},
		% [peers,{peer},network,http_client,shutdown] => #{},

		%---------------------------------------------------------------------
		% storage configuration
		%---------------------------------------------------------------------
		% [storage,{index},{type},state] => #{},
		% [storage,{index},{type},public_key] => #{},

		%---------------------------------------------------------------------
		% semaphores
		%---------------------------------------------------------------------
		#{
			configuration_key => [semaphores, get_chunk]
		},
		#{
			configuration_key => [semaphores, get_and_path_chunk]
		},
		#{
			configuration_key => [semaphores, get_tx_data]
		},
		#{
			configuration_key => [semaphores, post_chunk]
		},
		#{
			configuration_key => [semaphores, get_block_index]
		},
		#{
			configuration_key => [semaphores, get_wallet_list]
		},
		#{
		  	configuration_key => [semaphores, get_sync_record]
		},
		#{
			 configuration_key => [semaphores, post_tx]
		},
		#{
			configuration_key => [semaphores, get_reward_history]
		},
		#{
		  	configuration_key => [semaphores, get_tx]
	  	}

		%---------------------------------------------------------------------
		% webhooks
		%---------------------------------------------------------------------
		% [webhooks, {event}, {event_type}, url] => #{},
		% [webhooks, {event}, {event_type}, headers] => #{}

		%---------------------------------------------------------------------
		% coordinated mining configuration
		%---------------------------------------------------------------------
	].

