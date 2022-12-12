{ config, lib, pkgs, ... }:

let
  inherit (lib) literalExpression mkEnableOption mkIf mkOption types;
  cfg = config.services.arweave;
  defaultUser = "arweave";
in
{
  options.services.arweave = {

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
      type = types.str;
      default = "";
      example = [ "http://domain-or-ip.com:1984" ];
      description = ''
        A trusted peer to fetch VDF outputs from
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
      default = pkgs.arweave;
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

    metricsDir = mkOption {
      type = types.path;
      default = "/var/lib/arweave/metrics";
      description = ''
        Directory path for node metric outputs
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
      default = defaultUser;
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
        List of paths to textfiles containing blacklisted txids
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

    headerSyncJobs = mkOption {
      type = types.int;
      default = 10;
      description = "The pace for which to sync up with historical headers.";
    };

    syncJobs = mkOption {
      type = types.int;
      default = 100;
      description = "The pace for which to sync up with historical data.";
    };

    diskPoolJobs = mkOption {
      type = types.int;
      default = 100;
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
      default = [];
      description = "A rate limiter to prevent the node from receiving too many http requests over 1 minute period.";
    };

    maxConnections = mkOption {
      type = types.int;
      default = 1024;
      description = "Maximum allowed TCP connections.";
    };
  };

  config = mkIf cfg.enable (
    let configFile =
          pkgs.writeText "config.json" (builtins.toJSON {
            data_dir = cfg.dataDir;
            metrics_dir = cfg.metricsDir;
            start_from_block_index = cfg.startFromBlockIndex;
            transaction_blacklists = cfg.transactionBlacklists;
            transaction_whitelists = cfg.transactionWhitelists;
            max_disk_pool_buffer_mb = cfg.maxDiskPoolBufferMb;
            max_disk_pool_data_root_buffer_mb = cfg.maxDiskPoolDataRootBufferMb;
            block_pollers = cfg.blockPollers;
            polling = cfg.polling;
            tx_validators = cfg.txValidators;
            disable = cfg.featuresDisable;
            header_sync_jobs = cfg.headerSyncJobs;
            sync_jobs = cfg.syncJobs;
            disk_pool_jobs = cfg.diskPoolJobs;
            debug = cfg.debug;
            packing_rate = cfg.packingRate;
            vdf_server_trusted_peer = cfg.vdfServerTrustedPeer;
            semaphores = {
              get_chunk = cfg.maxParallelGetChunkRequests;
              get_and_pack_chunk = cfg.maxParallelGetAndPackChunkRequests;
              get_tx_data = cfg.maxParallelGetTxDataRequests;
              post_chunk = cfg.maxParallelPostChunkRequests;
              get_block_index = cfg.maxParallelBlockIndexRequests;
              get_wallet_list = cfg.maxParallelWalletListRequests;
              get_sync_record = cfg.maxParallelGetSyncRecord;
              arql = 10;
              gateway_arql = 10;
            };
            requests_per_minute_limit = cfg.requestsPerMinuteLimit;
            max_connections = cfg.maxConnections;

            requests_per_minute_limit_by_ip = lib.lists.foldr (ipObj: acc: acc // {
              "${ipObj.ip}" = {
                chunk = ipObj.chunkLimit;
                data_sync_record = ipObj.dataSyncRecordLimit;
                default = ipObj.defaultLimit;
              };
            }) {} cfg.requestsPerMinuteLimitByIp;
          });
    in {
      systemd.services.arweave = {
        description = "Arweave Node Service";
        after = [ "network.target" ];
        environment = {};
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          User = cfg.user;
          Group = cfg.group;
          WorkingDirectory = "${cfg.package}";
          Type = "forking";
          KillMode = "none";
          ExecStartPre = "${pkgs.bash}/bin/bash -c '(${pkgs.procps}/bin/pkill epmd || true) && (${pkgs.procps}/bin/pkill screen || true) && sleep 5 || true'";
          ExecStart = "${pkgs.screen}/bin/screen -dmS arweave ${cfg.package}/bin/start-nix config_file ${configFile} ${builtins.concatStringsSep " " (builtins.concatMap (p: ["peer" p]) cfg.peer)} ${builtins.concatStringsSep " " (builtins.concatMap (p: ["vdf_client_peer" p]) cfg.vdfClientPeer)}";
          ExecStop = "${pkgs.bash}/bin/bash -c '${pkgs.procps}/bin/pkill beam || true; sleep 15'";
          RestartKillSignal = "SIGINT";
        };
      };
    });
}
