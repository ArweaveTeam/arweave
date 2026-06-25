%%% @doc Specs for the `gossip` option group. Options for
%%% governing peer-to-peer exchange of node state and data.
-module(arweave_config_options_gossip).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([set_max_duplicates/4]).
-include("arweave_config.hrl").

specs() ->
	[
		%-----------------------------------------------------
		% Block propagation
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [gossip, block, poll_interval],
			runtime => true,
			default => ?DEFAULT_POLLING_INTERVAL,
			type => pos_integer,
			legacy => polling,
			short_description =>
				<<"Frequency in seconds of polling peers for new "
				  "blocks.">>
		},
		#{
			enabled => true,
			option_key => [gossip, block, pollers],
			default => ?DEFAULT_BLOCK_POLLERS,
			type => pos_integer,
			legacy => block_pollers,
			short_description =>
				<<"Number of peer-polling jobs that look for new "
				  "blocks.">>
		},
		#{
			enabled => true,
			option_key => [gossip, block, max_peers],
			default => ?DEFAULT_MAX_BLOCK_PROPAGATION_PEERS,
			type => pos_integer,
			legacy => max_block_propagation_peers,
			short_description =>
				<<"Maximum number of best peers to propagate blocks "
				  "to.">>
		},
		#{
			enabled => true,
			option_key => [gossip, block, throttle_by_ip_interval],
			runtime => true,
			default => ?DEFAULT_BLOCK_THROTTLE_BY_IP_INTERVAL_MS,
			type => pos_integer,
			legacy => block_throttle_by_ip_interval,
			short_description =>
				<<"Milliseconds that must pass before we accept "
				  "another block from the same IP address.">>
		},
		#{
			enabled => true,
			option_key => [gossip, block, throttle_by_solution_interval],
			runtime => true,
			default => ?DEFAULT_BLOCK_THROTTLE_BY_SOLUTION_INTERVAL_MS,
			type => pos_integer,
			legacy => block_throttle_by_solution_interval,
			short_description =>
				<<"Milliseconds that must pass before we accept "
				  "another block with the same solution hash.">>
		},

		%-----------------------------------------------------
		% Transaction propagation
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [gossip, tx, max_emitters],
			default => ?NUM_EMITTER_PROCESSES,
			type => pos_integer,
			legacy => max_emitters,
			short_description =>
				<<"Number of transaction propagation processes to "
				  "spawn.">>,
			long_description =>
				<<"Must be at least 1.">>
		},
		#{
			enabled => true,
			option_key => [gossip, tx, max_peers],
			runtime => true,
			default => ?DEFAULT_MAX_PROPAGATION_PEERS,
			type => pos_integer,
			legacy => max_propagation_peers,
			short_description =>
				<<"Maximum number of peers to propagate transactions "
				  "to.">>
		},
		#{
			enabled => true,
			option_key => [gossip, tx, polling_enabled],
			runtime => true,
			default => true,
			type => boolean,
			legacy => tx_polling_enabled,
			short_description =>
				<<"Run the periodic external-tx polling job.">>
		},
		#{
			enabled => true,
			option_key => [gossip, tx, post_timeout],
			runtime => true,
			default => ?DEFAULT_POST_TX_TIMEOUT,
			type => pos_integer,
			legacy => post_tx_timeout,
			short_description =>
				<<"Seconds to wait for an available tx-validation "
				  "process before dropping a POST /tx request.">>,
			long_description =>
				<<"Override the number of validation processes by "
				  "setting the `post_tx` key in the `semaphores` "
				  "object in the configuration file.">>
		},

		%-----------------------------------------------------
		% Data roots metadata exchange
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [gossip, data_roots, syncing_enabled],
			runtime => true,
			default => true,
			type => boolean,
			legacy => enable_data_roots_syncing,
			short_description =>
				<<"Enable or disable background data-roots syncing.">>
		},
		#{
			enabled => true,
			option_key => [gossip, data_roots, max_duplicates],
			runtime => true,
			default => ?DEFAULT_MAX_DUPLICATE_DATA_ROOTS,
			legacy => max_duplicate_data_roots,
			short_description =>
				<<"Maximum number of duplicate data roots to inspect "
				  "when checking whether a posted chunk is already "
				  "synced.">>,
			long_description =>
				<<"Set to `infinity` to disable the limit.">>,
			handle_set =>
				fun arweave_config_options_gossip:set_max_duplicates/4
		},

		%-----------------------------------------------------
		% Header sync
		%-----------------------------------------------------
		#{
			enabled => true,
			option_key => [gossip, header_sync_jobs],
			default => ?DEFAULT_HEADER_SYNC_JOBS,
			type => pos_integer,
			legacy => header_sync_jobs,
			short_description =>
				<<"Number of header-syncing jobs.">>,
			long_description =>
				<<"Each job periodically picks the latest not-synced "
				  "block header and downloads it from peers.">>
		},
		#{
			enabled => true,
			option_key => [gossip, header_cache_size],
			runtime => true,
			default => ?DISK_CACHE_SIZE,
			type => pos_integer,
			legacy => disk_cache_size,
			short_description =>
				<<"Maximum size in MiB allocated for storing recent "
				  "block and transaction headers.">>,
			long_description =>
				<<"Legacy JSON / CLI spelling: `disk_cache_size_mb`.">>
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Manage block, transaction, and metadata exchange with peers.">>.

%% @doc `max_duplicates` transform: a non-negative integer or the
%% atom / binary `infinity`.
set_max_duplicates(_K, V, _S, _A) ->
	case decode_max_duplicates(V) of
		{ok, Decoded} -> {store, Decoded};
		{error, _} = Err -> Err
	end.

decode_max_duplicates(N) when is_integer(N), N >= 0 -> {ok, N};
decode_max_duplicates(infinity) -> {ok, infinity};
decode_max_duplicates(<<"infinity">>) -> {ok, infinity};
decode_max_duplicates(V) when is_binary(V) ->
	%% Env vars arrive as binary strings; parse numerics here since
	%% the spec has no `type' field to coerce them.
	try binary_to_integer(V) of
		N when N >= 0 -> {ok, N};
		_ -> {error, {bad_max_duplicates, V}}
	catch
		_:_ -> {error, {bad_max_duplicates, V}}
	end;
decode_max_duplicates(V) ->
	{error, {bad_max_duplicates, V}}.
