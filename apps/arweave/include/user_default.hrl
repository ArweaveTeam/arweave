%%
%% This file is only intended to be included into user_default.erl file.
%% The reason to incluide these headers into user_default module is to
%% enable records to be rendered properly in the REPL.
%% It might be a good idea to also include some third-party libraries headers
%% here as well (e.g. cowboy' request, etc.)
%%

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_blacklist_middleware.hrl").
-include_lib("arweave/include/ar_block.hrl").
-include_lib("arweave/include/ar_chain_stats.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("arweave/include/ar_inflation.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_p3.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_poa.hrl").
-include_lib("arweave/include/ar_pool.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_sync_buckets.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_verify_chunks.hrl").
-include_lib("arweave/include/ar_wallets.hrl").
