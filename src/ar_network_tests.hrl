%%% Defines network 'integration' test sepcifications, used by the ar_test_sup.

-include("ar.hrl").

-record(network_test, {
	name,
	timeout = 15 * 60, % in seconds
	% Spawn settings
	num_miners = 10,
	num_clients = 20,
	% Simulated client settings
	client_connections = 3,
	client_action_time = 600,
	client_max_tx_len = 1024 * 1024,
	% Miner settings
	miner_connections = 10,
	miner_loss_probability = 0.025,
	miner_max_latency = 200,
	miner_xfer_speed = 512 * 1024,
	miner_delay = ?DEFAULT_MINING_DELAY * 10
}).

-define(NETWORK_TESTS,
	[
		#network_test { name = default },
		#network_test { name = no_clients, num_clients = 0 },
		#network_test {
			name = realistic,
			num_miners = 10,
			num_clients = 20,
			client_max_tx_len = 8,
			miner_connections = 5,
			miner_delay = ?DEFAULT_MINING_DELAY * 1000
		}
	]
).
