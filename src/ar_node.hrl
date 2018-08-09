-record(state, {
	hash_list = not_joined, % current full hashlist
	wallet_list = [], % current up to date walletlist
	floating_wallet_list = [], % up to date walletlist with new txs applied
	height = 0, % current height of the blockweave
	gossip, % Gossip protcol state
	txs = [], % set of new txs to be mined into the next block
	miner, % PID of the mining process
	mining_delay = 0, % delay on mining, used for netework simulation
	automine = false, % boolean dictating if a node should automine
	reward_addr = unclaimed, % reward address for mining a new block
	trusted_peers = [], % set of trusted peers used to join on
	waiting_txs = [], % set of txs on timeout whilst network distribution occurs
	potential_txs = [], % set of valid but contradictory txs
	tags = [], % nodes tags to apply to a block when mining
	reward_pool = 0, % current mining rewardpool of the weave
	diff = 0, % current mining difficulty of the weave (no. of preceeding zero)
	last_retarget, % timestamp at which the last difficulty retarget occurred
	weave_size = 0, % current size of the weave in bytes (only inc. data tx size)
	task_queue = queue:new(), % queue of state-changing tasks
	worker_pid = undefined % pid of ar_node_worker server
}).
