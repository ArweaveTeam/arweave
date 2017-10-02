-module(ar_adt_node).
-export([start/0, start/1, start/2, stop/1]).
-export([get_blocks/1, get_block/2, get_balance/2, generate_data_segment/2]).
-export([mine/1, automine/1, truncate/1, add_tx/2, add_peers/2]).
-export([set_loss_probability/2, set_delay/2, set_mining_delay/2, set_xfer_speed/2]).
-export([apply_txs/2, validate/4, validate/5, find_recall_block/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Blockweave maintaining nodes in the ArkChain system.

-record(state, {
	block_list,
	hash_list = [],
	wallet_list = [],
	gossip,
	txs = [],
	miner,
	mining_delay = 0,
	recovery = undefined,
	automine = false
}).

%% Start a node, optionally with a list of peers.
start() -> start([]).
start(Peers) -> start(Peers, undefined).
start(Peers, BlockList) -> start(Peers, BlockList, 0).
start(Peers, BlockList, MiningDelay) ->
	adt_simple:start(
		?MODULE,
		#state {
			gossip = ar_gossip:init(Peers),
			block_list = BlockList,
			wallet_list =
				case BlockList of
					undefined -> [];
					_ -> (find_sync_block(BlockList))#block.wallet_list
				end,
			mining_delay = MiningDelay
		}
	).

