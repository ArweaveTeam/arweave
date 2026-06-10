%%% @doc Specs for the `misc` option group. Options for
%%% node-wide settings that multiple subsystems depend on.
-module(arweave_config_options_misc).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [config_file],
			default => not_set,
			type => path,
			runtime => false,
			deprecated => false,
			required => false,
			short_description =>
				<<"Load the configuration from the specified file.">>
		},
		#{
			enabled => true,
			option_key => [data_dir],
			default => "./data",
			runtime => false,
			type => string,
			deprecated => false,
			legacy => data_dir,
			required => true,
			short_description =>
				<<"The directory for storing the weave and generated wallets.">>
		},
		#{
			enabled => true,
			option_key => [log_dir],
			default => ?LOG_DIR,
			runtime => false,
			type => string,
			deprecated => false,
			legacy => log_dir,
			required => false,
			short_description =>
				<<"The directory for logs.">>,
			long_description =>
				<<"The RocksDB logs are written to logs/rocksdb/.">>
		},
	 	#{
			enabled => true,
			option_key => [debug],
			default => false,
			runtime => true,
			type => boolean,
			deprecated => false,
			legacy => debug,
			required => false,
			short_description => <<"Enable extended logging.">>,
			long_description =>
				<<"Lowers the log level to debug across the node and "
				  "starts the dedicated debug log handler.">>,
			handle_set => fun
				(_K, V, _S = #{ config := #{ debug := Old }}, _) ->
					case {V, Old} of
						{true, true} ->
							ignore;
						{false, false} ->
							ignore;
						{false, true} ->
							logger:set_application_level(arweave_config, info),
							logger:set_application_level(arweave, info),
							ar_logger:stop_handler(arweave_debug),
							{store, V};
						{true, false} ->
							logger:set_application_level(arweave_config, debug),
							logger:set_application_level(arweave, debug),
							ar_logger:start_handler(arweave_debug),
							{store, V}
					end;
				(_K, V, _S, _) ->
					logger:set_application_level(arweave_config, debug),
					logger:set_application_level(arweave, debug),
					ar_logger:start_handler(arweave_debug),
					{store, V}
			end
		},
		#{
			enabled => true,
			option_key => [disable_device_limit],
			runtime => true,
			default => false,
			type => boolean,
			legacy => disable_replica_2_9_device_limit,
			short_description =>
				<<"Disable the per-physical-disk worker limit.">>,
			long_description =>
				<<"By default, at most one worker is active per "
				  "physical disk at a time. Setting this flag removes "
				  "that limit, allowing multiple workers to be active "
				  "on a given physical disk.">>,
			handle_set => fun(_K, V, _S, _A) ->
				ok = ar_device_lock:set_disable_device_limit(V),
				{store, V}
			end
		},
		#{
			enabled => true,
			option_key => [chunk_storage_file_size],
			default => ?CHUNK_GROUP_SIZE,
			type => pos_integer,
			legacy => chunk_storage_file_size,
			short_description =>
				<<"Size in bytes of an individual chunk storage "
				  "file.">>,
			long_description =>
				<<"Changing this on an existing weave is "
				  "unsupported.">>
		},
		#{
			enabled => true,
			option_key => [port],
			default => ?DEFAULT_HTTP_IFACE_PORT,
			type => pos_integer,
			legacy => port,
			short_description =>
				<<"Local port to use for mining.">>,
			long_description =>
				<<"This port must be reachable by remote peers.">>
		},
		#{
			enabled => true,
			option_key => [internal_api_secret],
			runtime => true,
			default => not_set,
			legacy => internal_api_secret,
			short_description =>
				<<"Enable the internal API endpoints, only "
				  "accessible with this secret.">>
		},
		#{
			enabled => true,
			option_key => [disk_space_check_frequency],
			runtime => true,
			default => ?DISK_SPACE_CHECK_FREQUENCY_MS,
			type => pos_integer,
			legacy => disk_space_check_frequency,
			short_description =>
				<<"Frequency in milliseconds of querying the OS for "
				  "available disk space.">>,
			long_description =>
				<<"Used to decide whether to continue syncing "
				  "historical data or clean up some space. Legacy "
				  "JSON / CLI `disk_space_check_frequency` takes "
				  "seconds.">>
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Manage node behavior not covered by other groups.">>.

