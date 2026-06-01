%%% @doc Specs for the `semaphores` option group. Options for
%%% bounding concurrent HTTP work inside the node.
%%%
%%% One spec per semaphore name; each spec carries its own
%%% compile-time default. Names are a closed set defined in
%%% `default_limits/0' — unknown names are rejected by the registry
%%% at set time because no spec matches.
%%%
%%% `legacy_map/0' reconstructs the legacy `#{atom() => integer()}' map
%%% shape via `arweave_config:get_all_with_prefix/1'.
-module(arweave_config_options_semaphores).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	legacy_map/0,
	write_legacy_map/1,
	validate/0
]).
-include("arweave_config.hrl").

specs() ->
	[spec_for(Name, Limit) ||
		{Name, Limit} <- maps:to_list(default_limits())].

spec_for(Name, Limit) ->
	#{
		enabled => true,
		option_key => [semaphores, Name, limit],
		type => pos_integer,
		default => Limit,
		short_description =>
			<<"Maximum in-flight HTTP requests for ",
			  (atom_to_binary(Name))/binary, ".">>
	}.

group_description() ->
	<<"Limit HTTP endpoint concurrency.">>.

validate() ->
	ok.

legacy_map() ->
	maps:from_list(
		[{Name, Value}
		 || {[semaphores, Name, limit], Value} <-
			arweave_config:get_all_with_prefix([semaphores])]).

%% @doc Compile-time default limit per semaphore name. Single source
%% of truth — `specs/0' and the legacy bridges derive everything else
%% from this map.
default_limits() ->
	#{
		get_chunk => ?MAX_PARALLEL_GET_CHUNK_REQUESTS,
		get_and_pack_chunk => ?MAX_PARALLEL_GET_AND_PACK_CHUNK_REQUESTS,
		get_tx_data => ?MAX_PARALLEL_GET_TX_DATA_REQUESTS,
		post_chunk => ?MAX_PARALLEL_POST_CHUNK_REQUESTS,
		get_block_index => ?MAX_PARALLEL_BLOCK_INDEX_REQUESTS,
		get_wallet_list => ?MAX_PARALLEL_WALLET_LIST_REQUESTS,
		get_sync_record => ?MAX_PARALLEL_GET_SYNC_RECORD_REQUESTS,
		post_tx => ?MAX_PARALLEL_POST_TX_REQUESTS,
		get_reward_history => ?MAX_PARALLEL_REWARD_HISTORY_REQUESTS,
		get_tx => ?MAX_PARALLEL_GET_TX_REQUESTS,
		get_data_roots => ?MAX_PARALLEL_GET_DATA_ROOTS_REQUESTS
	}.

%% @doc Apply a legacy `#{Name => Limit}' map, clearing all per-name
%% leaves first so unmentioned names revert to their compile-time
%% default. Unknown names are silently skipped, matching the legacy
%% parser's drop-with-warning behaviour for bad entries.
-spec write_legacy_map(map()) -> ok.
write_legacy_map(Map) when is_map(Map) ->
	clear_legacy_entries(),
	Known = maps:keys(default_limits()),
	maps:foreach(
		fun(Name, Limit) ->
			case lists:member(Name, Known) of
				true ->
					_ = arweave_config:set(
						[semaphores, Name, limit], Limit),
					ok;
				false ->
					ok
			end
		end, Map),
	ok.

%% @doc Reset all `[semaphores, ...]' leaves so they read their
%% compile-time defaults. Goes through the store directly because the
%% registry has no delete API and a legacy-map reload must roll
%% unmentioned names back to defaults.
clear_legacy_entries() ->
	Items = arweave_config:get_all_with_prefix([semaphores]),
	[arweave_config_store:delete(Key) || {Key, _Value} <- Items],
	ok.
