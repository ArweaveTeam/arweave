%%% @doc Specs for the `sync` option group. Options for
%%% controlling how the node discovers and fetches chunk data.
-module(arweave_config_options_sync).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
	[
		#{
			enabled => true,
			option_key => [sync, cache_size_limit],
			type => pos_integer,
			legacy => data_cache_size_limit,
			short_description =>
				<<"Maximum number of data chunks kept in "
				  "memory by the syncing processes (approximate).">>
		},
		#{
			enabled => true,
			option_key => [sync, max_concurrent_peer_scans],
			default => ?DEFAULT_DATA_DISCOVERY_MAX_CONCURRENT_PEER_SCANS,
			type => pos_integer,
			legacy => data_discovery_max_concurrent_peer_scans,
			short_description =>
				<<"Maximum concurrent peer-discovery scanners across "
				  "all (Peer, Mode) pairs.">>
		},
		#{
			enabled => true,
			option_key => [sync, request_packed_chunks],
			default => false,
			type => boolean,
			legacy => data_sync_request_packed_chunks,
			short_description =>
				<<"Request packed chunks from peers during data sync.">>
		},
		#{
			enabled => true,
			option_key => [sync, local_peers_only],
			default => false,
			type => boolean,
			legacy => sync_from_local_peers_only,
			short_description =>
				<<"Sync data (not headers) only from local-network "
				  "peers configured via the local_peer option.">>
		},
		#{
			enabled => true,
			option_key => [sync, jobs],
			default => ?DEFAULT_SYNC_JOBS,
			type => pos_integer,
			legacy => sync_jobs,
			short_description =>
				<<"Number of data-syncing jobs to run.">>,
			long_description =>
				<<"Each job periodically picks a byte range and "
				  "downloads it from peers.">>
		}
	].

validate() ->
	ok.

group_description() ->
	<<"Manage weave data discovery and synchronization.">>.
