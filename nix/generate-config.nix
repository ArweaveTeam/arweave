{ arweaveConfig, pkgs, ... }:

let
  inherit (pkgs) lib;
  filterTopLevelNulls = set:
    let
      isNotNull = value: value != null;
    in
    lib.filterAttrs (name: value: isNotNull value) set;
in
pkgs.writeText "config.json" (builtins.toJSON (filterTopLevelNulls {
  data_dir = arweaveConfig.dataDir;
  log_dir = arweaveConfig.logDir;
  storage_modules = arweaveConfig.storageModules;
  start_from_block_index = arweaveConfig.startFromBlockIndex;
  transaction_blacklists = arweaveConfig.transactionBlacklists;
  transaction_whitelists = arweaveConfig.transactionWhitelists;
  transaction_blacklist_urls = arweaveConfig.transactionBlacklistURLs;
  max_disk_pool_buffer_mb = arweaveConfig.maxDiskPoolBufferMb;
  max_disk_pool_data_root_buffer_mb = arweaveConfig.maxDiskPoolDataRootBufferMb;
  max_nonce_limiter_validation_thread_count = arweaveConfig.maxVDFValidationThreadCount;
  block_pollers = arweaveConfig.blockPollers;
  polling = arweaveConfig.polling;
  tx_validators = arweaveConfig.txValidators;
  disable = arweaveConfig.featuresDisable;
  enable = arweaveConfig.featuresEnable;
  header_sync_jobs = arweaveConfig.headerSyncJobs;
  sync_jobs = arweaveConfig.syncJobs;
  disk_pool_jobs = arweaveConfig.diskPoolJobs;
  debug = arweaveConfig.debug;
  packing_rate = arweaveConfig.packingRate;
  block_throttle_by_ip_interval = arweaveConfig.blockThrottleByIPInterval;
  block_throttle_by_solution_interval = arweaveConfig.blockThrottleBySolutionInterval;
  semaphores = {
    get_chunk = arweaveConfig.maxParallelGetChunkRequests;
    get_and_pack_chunk = arweaveConfig.maxParallelGetAndPackChunkRequests;
    get_tx_data = arweaveConfig.maxParallelGetTxDataRequests;
    post_chunk = arweaveConfig.maxParallelPostChunkRequests;
    get_block_index = arweaveConfig.maxParallelBlockIndexRequests;
    get_wallet_list = arweaveConfig.maxParallelWalletListRequests;
    get_sync_record = arweaveConfig.maxParallelGetSyncRecord;
    arql = 10;
    gateway_arql = 10;
  };
  requests_per_minute_limit = arweaveConfig.requestsPerMinuteLimit;
  max_connections = arweaveConfig.maxConnections;

  requests_per_minute_limit_by_ip = lib.lists.foldr
    (ipObj: acc: acc // {
      "${ipObj.ip}" = {
        chunk = ipObj.chunkLimit;
        data_sync_record = ipObj.dataSyncRecordLimit;
        default = ipObj.defaultLimit;
      };
    })
    { }
    arweaveConfig.requestsPerMinuteLimitByIp;
}))
