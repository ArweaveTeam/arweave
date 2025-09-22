%%%===================================================================
%%% Arweave header file defining default values for most of Arweave
%%% parameters. The convention used is the same than the one used for
%%% naming Arweave parameters.
%%%===================================================================

%%--------------------------------------------------------------------
%% Global Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_GLOBAL_INIT, false).
-define(DEFAULT_GLOBAL_DEBUG, false).
-define(DEFAULT_GLOBAL_DATA_DIRECTORY, ".").
-define(DEFAULT_GLOBAL_LOGGING_DIRECTORY, "logs").

%%--------------------------------------------------------------------
%% Network Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_GLOBAL_NETWORK_SHUTDOWN_MODE, shutdown).
-define(DEFAULT_GLOBAL_NETWORK_TCP_SHUTDOWN_CONNECTION_TIMEOUT, 30).
-define(DEFAULT_GLOBAL_NETWORK_SOCKET_BACKEND, inet).

% Arweave API Configuration (cowboy).
-define(DEFAULT_GLOBAL_NETWORK_API_LISTEN_PORT, 1984).
% -define(DEFAULT_GLOBAL_NETWORK_API_LISTEN_ADDRESS, "0.0.0.0").
-define(DEFAULT_GLOBAL_NETWORK_API_HTTP_ACTIVE_N, 100).
-define(DEFAULT_GLOBAL_NETWORK_API_HTTP_INACTIVITY_TIMEOUT, 300_000).
-define(DEFAULT_GLOBAL_NETWORK_API_HTTP_LINGER_TIMEOUT, 1000).
-define(DEFAULT_GLOBAL_NETWORK_API_HTTP_REQUEST_TIMEOUT, 5000).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_BACKLOG, 1024).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_DELAY_SEND, false).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_KEEPALIVE, true).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_LINGER, false).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_LINGER_TIMEOUT, 0).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_LISTENER_SHUTDOWN, 5000).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_MAX_CONNECTIONS, 1024).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_NODELAY, true).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_NUM_ACCEPTORS, 10).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_SEND_TIMEOUT, 15_000).
-define(DEFAULT_GLOBAL_NETWORK_API_TCP_SEND_TIMEOUT_CLOSE, true).

% Arweave Client Configuration (gun).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_HTTP_CLOSING_TIMEOUT, 15_000).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_HTTP_KEEPALIVE, 60_000).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_DELAY_SEND, false).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_KEEPALIVE, true).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_LINGER_TIMEOUT, 0).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_LINGER, false).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_NODELAY, true).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_SEND_TIMEOUT_CLOSE, true).
-define(DEFAULT_GLOBAL_NETWORK_CLIENT_TCP_SEND_TIMEOUT, 15_000).

%%--------------------------------------------------------------------
%% Semaphores Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_SEMAPHORES_GET_AND_UNPACK_CHUNK, 1).
-define(DEFAULT_SEMAPHORES_GET_BLOCK_INDEX, 1).
-define(DEFAULT_SEMAPHORES_GET_CHUNK, 100).
-define(DEFAULT_SEMAPHORES_GET_REWARD_HISTORY, 1).
-define(DEFAULT_SEMAPHORES_GET_SYNC_RECORD, 10).
-define(DEFAULT_SEMAPHORES_GET_TX, 1).
-define(DEFAULT_SEMAPHORES_GET_TX_DATA, 1).
-define(DEFAULT_SEMAPHORES_GET_WALLET_LIST, 1).
-define(DEFAULT_SEMAPHORES_POST_CHUNK, 100).
-define(DEFAULT_SEMAPHORES_POST_TX, 20).

%%--------------------------------------------------------------------
%% VDF Configuration Macros.
%%--------------------------------------------------------------------
% instead of "mode" using "backend"?
-define(DEFAULT_VDF_MODE, openssl).

%%--------------------------------------------------------------------
%% Blocks Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_BLOCKS_POLLERS, 10).

%%--------------------------------------------------------------------
%% Chunks Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_CHUNKS_VERIFY_SAMPLES, false).
-define(DEFAULT_CHUNKS_VERIFY, false).

%%--------------------------------------------------------------------
%% WebUI Configuration Macros.
%%--------------------------------------------------------------------
-define(DEFAULT_WEBUI_ENABLED, false).
-define(DEFAULT_WEBUI_LISTEN_PORT, 4891).
-define(DEFAULT_WEBUI_LISTEN_ADDRESS, "127.0.0.1").

