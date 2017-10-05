%%% Defines network 'integration' test sepcifications, used by the ar_test_sup.

-include("ar.hrl").

-record(network_test, {
	name,
	check_time = 10, % How frequently to check the net in seconds
	failure_time = ?TARGET_TIME * 10, % When to restart the net (in seconds)
	stagger_time = 3000, % Maximum period (in ms) to leave between node spawns.
	% Spawn settings
	num_miners = 10,
	num_clients = 20,
	% Simulated client settings
	client_connections = 3,
	client_action_time = 600,
	client_max_tx_len = 1024,
	% Miner settings
	miner_connections = 10,
	miner_loss_probability = 0.025,
	miner_max_latency = 200,
	miner_xfer_speed = 512 * 1024,
	miner_delay = calculate
}).

%% Representation of live tests.
-record(test_run, {
	name,
	monitor,
	miners,
	clients,
	start_time = erlang:universaltime(),
	fail_time = undefined,
	log = undefined
}).

-define(NETWORK_TESTS,
	[
%		#network_test { name = default }
%		#network_test { name = no_clients, num_clients = 0 },
		#network_test {
			name = realistic,
			num_miners = 500,
			client_action_time = 900,
			num_clients = 100,
			client_max_tx_len = 8,
			miner_connections = 8,
			stagger_time = 300
		}
	]
).
