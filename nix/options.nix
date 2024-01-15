{ lib, defaultArweaveConfigFile ? null, defaultArweavePackage ? null }:
let
  inherit (lib) mkEnableOption literalExpression mkOption mkOptionals mkForce mkForceOption types;
in
{

  enable = mkEnableOption ''
    Enable arweave node as systemd service
  '';

  peer = mkOption {
    type = types.nonEmptyListOf types.str;
    default = [ ];
    example = [ "http://domain-or-ip.com:1984" ];
    description = ''
      List of primary node peers
    '';
  };

  vdfServerTrustedPeer = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "http://domain-or-ip.com:1984" ];
    description = ''
      List of trusted peers to fetch VDF outputs from
    '';
  };

  vdfClientPeer = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "http://domain-or-ip.com:1984" ];
    description = ''
      List of peers to serve VDF updates to
    '';
  };

  package = mkOption {
    type = types.package;
    default = defaultArweavePackage;
    defaultText = literalExpression "pkgs.arweave";
    example = literalExpression "pkgs.arweave";
    description = ''
      The Arweave expression to use
    '';
  };

  dataDir = mkOption {
    type = types.path;
    default = "/arweave-data";
    description = ''
      Data directory path for arweave node.
    '';
  };

  logDir = mkOption {
    type = types.path;
    default = "/var/lib/arweave/logs";
    description = ''
      Logging directory path.
    '';
  };

  crashDumpsDir = mkOption {
    type = types.path;
    default = "/var/lib/arweave/dumps";
    description = ''
      Crash dumps directory path.
    '';
  };

  erlangCookie = mkOption {
    type = with types; nullOr str;
    default = null;
    description = ''
      Erlang cookie for distributed erlang.
    '';
  };

  storageModules = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "0,1000000000000,unpacked" "1,1000000000000,unpacked" ];
    description = ''
      List of configured storage modules.
    '';
  };

  startFromBlockIndex = mkOption {
    type = types.bool;
    default = false;
    description = "If set, starts from the locally stored state.";
  };

  debug = mkOption {
    type = types.bool;
    default = false;
    description = "Enable debug logging.";
  };

  user = mkOption {
    type = types.str;
    default = "arweave";
    description = "Run Arweave Node under this user.";
  };

  group = mkOption {
    type = types.str;
    default = "users";
    description = "Run Arweave Node under this group.";
  };

  transactionBlacklists = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "/user/arweave/blacklist.txt" ];
    description = ''
      List of paths to textfiles containing blacklisted txids and/or byte ranges
    '';
  };

  transactionBlacklistURLs = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "http://example.org/blacklist.txt" ];
    description = ''
      List of URLs of the endpoints serving blacklisted txids and/or byte ranges
    '';
  };

  transactionWhitelists = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "/user/arweave/whitelist.txt" ];
    description = ''
      List of paths to textfiles containing whitelisted txids
    '';
  };

  maxDiskPoolBufferMb = mkOption {
    type = types.int;
    default = 2000;
    description = "Max disk-pool buffer size in mb.";
  };

  maxDiskPoolDataRootBufferMb = mkOption {
    type = types.int;
    default = 500;
    description = "Max disk-pool data-root buffer size in mb.";
  };

  blockPollers = mkOption {
    type = types.int;
    default = 10;
    description = "The number of block polling jobs.";
  };

  polling = mkOption {
    type = types.int;
    default = 2;
    description = "The frequency of block polling, in seconds.";
  };

  blockThrottleByIPInterval = mkOption {
    type = types.int;
    default = 1000;
    description = "";
  };

  blockThrottleBySolutionInterval = mkOption {
    type = types.int;
    default = 2000;
    description = "";
  };

  txValidators = mkOption {
    type = types.int;
    default = 10;
    description = "The number of transaction validation jobs.";
  };

  packingRate = mkOption {
    type = types.int;
    default = 30;
    description = "The maximum number of chunks the node will pack per second.";
  };

  featuresDisable = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "packing" ];
    description = ''
      List of features to disable.
    '';
  };

  featuresEnable = mkOption {
    type = types.listOf types.str;
    default = [ ];
    example = [ "repair_rocksdb" ];
    description = ''
      List of features to enable.
    '';
  };

  headerSyncJobs = mkOption {
    type = types.int;
    default = 10;
    description = "The pace for which to sync up with historical headers.";
  };

  syncJobs = mkOption {
    type = types.int;
    default = 10;
    description = "The pace for which to sync up with historical data.";
  };

  diskPoolJobs = mkOption {
    type = types.int;
    default = 50;
    description = "The number of disk pool jobs to run.";
  };

  maxParallelGetChunkRequests = mkOption {
    type = types.int;
    default = 100;
    description = "As semaphore, the max amount of parallel get chunk requests to perform.";
  };

  maxParallelGetAndPackChunkRequests = mkOption {
    type = types.int;
    default = 10;
    description = "As semaphore, the max amount of parallel get chunk and pack requests to perform.";
  };

  maxParallelGetTxDataRequests = mkOption {
    type = types.int;
    default = 10;
    description = "As semaphore, the max amount of parallel get transaction data requests to perform.";
  };

  maxParallelPostChunkRequests = mkOption {
    type = types.int;
    default = 100;
    description = "As semaphore, the max amount of parallel post chunk requests to perform.";
  };

  maxParallelBlockIndexRequests = mkOption {
    type = types.int;
    default = 2;
    description = "As semaphore, the max amount of parallel block index requests to perform.";
  };

  maxParallelWalletListRequests = mkOption {
    type = types.int;
    default = 2;
    description = "As semaphore, the max amount of parallel block index requests to perform.";
  };

  maxParallelGetSyncRecord = mkOption {
    type = types.int;
    default = 2;
    description = "As semaphore, the max amount of parallel get sync record requests to perform.";
  };

  maxVDFValidationThreadCount = mkOption {
    type = with types; nullOr int;
    default = null;
    description = ''
      The number of threads to use for VDF validation.
      Note that the default value (null) defaults in runtime
      to `max(1, (erlang:system_info(schedulers_online) div 2))).`
    '';
  };

  requestsPerMinuteLimit = mkOption {
    type = types.int;
    default = 2500;
    description = "A rate limiter to prevent the node from receiving too many http requests over 1 minute period.";
  };

  requestsPerMinuteLimitByIp = mkOption {
    type = types.listOf (types.submodule {
      options = {
        ip = mkOption {
          type = types.str;
          description = ''
            ip address of client to rate limit
          '';
        };
        chunkLimit = mkOption {
          type = types.int;
          description = ''
            rate of chunk data requests over 1 minute period to limit
          '';
        };
        dataSyncRecordLimit = mkOption {
          type = types.int;
          description = ''
            rate of sync_data_record requests over 1 minute period to limit
          '';
        };
        defaultLimit = mkOption {
          type = types.int;
          description = ''
            the default rate of requests over 1 minute period to limit
          '';
        };
      };
    });
    default = [ ];
    description = "A rate limiter to prevent the node from receiving too many http requests over 1 minute period.";
  };

  maxConnections = mkOption {
    type = types.int;
    default = 1024;
    description = "Maximum allowed TCP connections.";
  };

  configFile = mkOption {
    type = types.path;
    default = defaultArweaveConfigFile;
    description = "The generated Arweave config file";
  };

}
