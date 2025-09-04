-module(arweave_config_keys).
-compile(export_all).

% spec() -> #{
% 	% global configuration
% 	[global,data,directory] => #{
% 		module => arweave_config_global_data_directory
% 	},
% 	[global,data,jobs,sync] => #{
% 		legacy => [sync_jobs],
% 		runtime => true,
% 		description => ""
% 	},
% 	[global,data,jobs,headers,sync] => #{
% 		legacy => [header_sync_jobs],
% 		runtime => true,
% 		description => ""
% 	},
% 
% 	[global,debug] => #{
% 		legacy => [debug]
% 		description => "",
% 		type => boolean,
% 		check => fun(K, V) -> ok end
% 	},
% 
% 	% global chunks configuration
% 	[global,chunks,verify_samples] => #{
% 		legacy => [verify_samples]
% 	},
% 	[global, chunks, verify] => #{
% 		legacy => [verify]
% 	}
% 
% 	% global network configuration
% 	[global,network,http_api,tcp,max_connections] => #{
% 		legacy => [max_connections, 'http_api.tcp.max_connections']
% 	}
% 	[global,network,http_client,tcp,nodelay] => #{
% 		legacy => ['http_client.tcp.nodelay']
% 	},
% 	[global,network,http_client,tcp,send_timeout_close] => #{
% 		legacy => ['http_client.tcp.send_timeout_close']
% 	},
% 	[global,network,http_client,tcp,send_timeout] => #{
% 		legacy => ['http_client.tcp.send_timeout']
% 	},
% 	[global,network,http_client,tcp,linger_timeout] => #{
% 		legacy => ['http_client.tcp.linger_timeout']
% 	},
% 	[global,network,http_client,tcp,linger] => #{
% 		legacy => ['http_client.tcp.linger']
% 	},
% 	[global,network,http_client,tcp,keepalive] => #{
% 		legacy => ['http_client.tcp.keepalive']
% 	},
% 	[global,network,http_client,tcp,delay_send] => #{
% 		legacy => ['http_client.tcp.delay_send']
% 	},
% 	[global,network,http_client,http,keepalive] => #{
% 		legacy => ['http_client.http.keepalive']
% 	},
% 	[global,network,http_client,http,closing_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,send_timeout_close] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,send_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,num_acceptors] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,nodelay] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,listener_shutdown] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,linger] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,linger_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,keepalive] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,idle_timeout_seconds] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,backlog] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,tcp,delay_send] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,http,request_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,http,linger_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,http,inactivity_timeout] => #{
% 		legacy => []
% 	},
% 	[global,network,http_api,http,active_n] => #{
% 		legacy => []
% 	},
% 
% 	% transactions
% 	[transactions, whitelist, {txid}] => #{},
% 	[transactions, blacklist, {txid}] => #{},
% 	[transactions, urls, blacklist, {url}] => #{}
% 	[transactions, urls, whitelist, {url}] => #{}
% 
% 	% peer configuration
% 	[peers, verify] => #{
% 		legacy => [verify]
% 	},
% 	[peers,{peer},block_gossip_peers] => #{},
% 	[peers,{peer},trusted] => #{},
% 	[peers,{peer},vdf,server] => #{},
% 	[peers,{peer},vdf,client] => #{},
% 	[peers,{peer},network,http_client,shutdown] => #{},
% 
% 	% storage configuration
% 	[storage,{index},{type},state] => #{},
% 	[storage,{index},{type},public_key] => #{},
% 
% 	% webhooks
% 	% [webhooks, {event}, {event_type}, url] => #{},
% 	% [webhooks, {event}, {event_type}, headers] => #{},
% }.
