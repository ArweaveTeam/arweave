%%%===================================================================
%%% @doc Module regrouping all parameters in one place, as map.
%%% @end
%%%===================================================================
-module(arweave_config_parameters).
-export([init/0]).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init() -> 
	[
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
			handle_get => fun(K) ->
				arweave_config_store:get([webui,enabled])
			end,
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
			environment => <<"AR_WEBUI_LISTEN_PORT">>,
			handle_get => fun(K) -> todo end,
			handle_set => fun(K,V,O) -> todo end
		},
		#{
			configuration_key => [webui,listen,address],
			short_argument => undefined,
			long_argument => [webui,listen,address],
			runtime => true,
			short_description => [],
			long_description => [],
			type => [ipv4,ipv6,unix_sock],
			environment => <<"AR_WEBUI_LISTEN_ADDRESS">>,
			handle_get => fun(K) -> todo end,
			handle_set => fun(K,V,O) -> todo end

		},
		% #config.init
		#{
			configuration_key => [global, init],
			default => false,
			short_argument => undefined,
			long_argument => <<"init">>,
			legacy => init,
			runtime => false,
			short_description => [
				<<"Start a new weave.">>
			],
			handle_get => fun(K) -> todo end,
			handle_set => fun(K,V,O) -> todo end
		},

		% #config.sync_jobs
		#{
			configuration_key => [global,data,jobs,sync],
			legacy => [sync_jobs],
			runtime => true,
			description => "",
			handle_get => fun(K) -> todo end,
			handle_set => fun(K,V,O) -> todo end
		},
		
		% #config.header_sync_jobs	
		#{
			configuration_key => [global,data,jobs,headers,sync],
			legacy => [header_sync_jobs],
			runtime => true,
			description => "",
			handle_get => fun(K) -> todo end,
			handle_set => fun(K,V,O) -> todo end
		}
	].
	
draft2() ->
	#{
		% config.verify_simples
		[global,chunks,verify,samples] => #{
			legacy => [verify_samples]
		},
	
		% config.verify
		[global,chunks,verify] => #{
			legacy => [verify]
		},
	
		% global network configuration
		[global,network,http_api,port] => #{},
		[global,network,http_api,tcp,max_connections] => #{
			legacy => [
				max_connections,
				'http_api.tcp.max_connections'
			]
		},

		[global,network,http_client,tcp,nodelay] => #{
			legacy => ['http_client.tcp.nodelay']
		},

		[global,network,http_client,tcp,send_timeout_close] => #{
			legacy => ['http_client.tcp.send_timeout_close']
		},

		[global,network,http_client,tcp,send_timeout] => #{
			legacy => ['http_client.tcp.send_timeout']
		},

		[global,network,http_client,tcp,linger_timeout] => #{
			legacy => ['http_client.tcp.linger_timeout']
		},

		[global,network,http_client,tcp,linger] => #{
			legacy => ['http_client.tcp.linger']
		},

		[global,network,http_client,tcp,keepalive] => #{
			legacy => ['http_client.tcp.keepalive']
		},

		[global,network,http_client,tcp,delay_send] => #{
			legacy => ['http_client.tcp.delay_send']
		},

		[global,network,http_client,http,keepalive] => #{
			legacy => ['http_client.http.keepalive']
		},

		[global,network,http_client,http,closing_timeout] => #{
			legacy => []
		},

		[global,network,http_api,tcp,send_timeout_close] => #{
			legacy => []
		},

		[global,network,http_api,tcp,send_timeout] => #{
			legacy => []
		},

		[global,network,http_api,tcp,num_acceptors] => #{
			legacy => []
		},

		[global,network,http_api,tcp,nodelay] => #{
			legacy => []
		},

		[global,network,http_api,tcp,listener_shutdown] => #{
			legacy => []
		},

		[global,network,http_api,tcp,linger] => #{
			legacy => []
		},

		[global,network,http_api,tcp,linger_timeout] => #{
			legacy => []
		},

		[global,network,http_api,tcp,keepalive] => #{
			legacy => []
		},

		[global,network,http_api,tcp,idle_timeout_seconds] => #{
			legacy => []
		},

		[global,network,http_api,tcp,backlog] => #{
			legacy => []
		},

		[global,network,http_api,tcp,delay_send] => #{
			legacy => []
		},

		[global,network,http_api,http,request_timeout] => #{
			legacy => []
		},

		[global,network,http_api,http,linger_timeout] => #{
			legacy => []
		},

		[global,network,http_api,http,inactivity_timeout] => #{
			legacy => []
		},

		[global,network,http_api,http,active_n] => #{
			legacy => []
		},
	
		% transactions
		[transactions, whitelist, {txid}] => #{},
		[transactions, blacklist, {txid}] => #{},
		[transactions, urls, blacklist, {url}] => #{},
		[transactions, urls, whitelist, {url}] => #{},
	
		% peer configuration
		[peers, verify] => #{
			legacy => [verify]
		},
		[peers,{peer},block_gossip_peers] => #{},
		[peers,{peer},trusted] => #{},
		[peers,{peer},vdf,server] => #{},
		[peers,{peer},vdf,client] => #{},
		[peers,{peer},network,http_client,shutdown] => #{},
	
		% storage configuration
		[storage,{index},{type},state] => #{},
		[storage,{index},{type},public_key] => #{},

		% semaphores
		[semaphores, get_chunk] => #{},
		[semaphores, get_and_path_chunk] => #{},
		[semaphores, get_tx_data] => #{},
		[semaphores, post_chunk] => #{},
		[semaphores, get_block_index] => #{},
		[semaphores, get_wallet_list] => #{},
		[semaphores, get_sync_record] => #{},
		[semaphores, post_tx] => #{},
		[semaphores, get_reward_history] => #{},
		[semaphores, get_tx] => #{}
	
		% webhooks
		% [webhooks, {event}, {event_type}, url] => #{},
		% [webhooks, {event}, {event_type}, headers] => #{},
	}.

