%%% @doc Public facade and lifecycle coordinator for Arweave configuration.
%%%
%%% The `arweave_config` app owns the node configuration. Most callers
%%% should treat this module as the public boundary.
%%%
%%% == Process topology ==
%%%
%%% Starting the application brings up a one-for-all supervisor with
%%% three long-lived pieces:
%%% - the value store (arweave_config_store)
%%% - the option-spec registry (arweave_config_options_registry)
%%% - the signal handler (arweave_config_signal_handler)
%%%
%%% This module is a pure public facade plus the OTP `application`
%%% callback. The registry owns spec lookup, mutation semantics, and
%%% the load/runtime lifecycle flag; the store owns the actual values.
%%%
%%% Configuration options are declared as maps in the
%%% `arweave_config_options_*` modules. These spec maps define the option
%%% name, defaults, types, read/write hooks, etc... 
%%%
%%% == Load and runtime lifecycle ==
%%%
%%% Startup begins in load mode, where every option may be written
%%% freely. `bootstrap/1` loads OS environment variables and then
%%% selects either the legacy CLI / `config.json` path or the new
%%% long-flag + JSON/YAML path. It returns a legacy-shaped
%%% proplist because the surrounding `ar` application still starts from
%%% that shape.
%%%
%%% `ar:start/1` loads that proplist back through this facade,
%%% normalizes the assembled state, starts the rest of the node, and
%%% then calls `runtime/0`. The runtime transition runs all contributor
%%% validators once and flips a one-way lifecycle flag. After that,
%%% only `runtime => true` specs accept writes; the rest reject them.
%%% Each successful runtime write is followed by validation and is
%%% rolled back if the full configuration becomes invalid.
%%%
%%% == Compatibility and aggregates ==
%%%
%%% Much of the node still speaks the historical config language.
%%% Specs with a `legacy` field map old atom names to canonical
%%% option_keys. Aggregate values such as peers, storage modules,
%%% webhooks, semaphores, and feature flags are stored as indexed
%%% per-leaf entries, so this facade also exposes typed helper
%%% functions for reading or replacing those aggregate views.
%%%
%%% In short: parsers and legacy bridges translate input into canonical
%%% option_keys; the registry enforces specs; the store holds values;
%%% normalization and validation turn the loaded state into a runtime
%%% contract for the rest of the node.
%%%
-module(arweave_config).
-compile(warnings_as_errors).
-vsn(1).
-behavior(application).
-export([
	get/1,
	get_all_with_prefix/1,
	is_runtime/0,
	runtime/0,
	set/2,
	load/1,
	with_test_config/1,
	snapshot/0,
	restore/1,
	start/0,
	stop/0
]).
%% Public API: peers
-export([
	get_peers/1,
	get_peer/1,
	clear_peers/1,
	replace_peers/2
]).
%% Public API: storage / repack / defrag modules
-export([
	storage_modules/0,
	repack_modules/0,
	defrag_modules/0,
	replace_storage_modules/1,
	replace_repack_modules/1
]).
%% Public API: webhooks, semaphores, features
-export([
	webhooks/0,
	replace_webhooks/1,
	semaphores/0,
	feature_enabled/1
]).
%% Public API: serialization / logging
-export([
	log/0
]).
%% Public API: bootstrap and orchestration helpers
-export([
	bootstrap/1,
	normalize/0,
	show_cli_help/0,
	parse_storage_module/1
]).
% application behavior callbacks.
-export([start/2, stop/1]).
-ifdef(AR_TEST).
-export([force_config/1, reset/0]).
-endif.
-compile({no_auto_import,[get/1]}).
-include_lib("kernel/include/logger.hrl").

%% @doc Start the `arweave_config` application and its dependencies.
-spec start() -> ok | {error, term()}.
start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, Dependencies} ->
			?LOG_DEBUG("arweave_config started dependencies: ~p", Dependencies),
			ok;
		Else ->
			Else
	end.

%% @doc Stop the `arweave_config` application.
-spec stop() -> ok.
stop() ->
	application:stop(?MODULE).

%% @doc Read a configuration value by its canonical option_key.
%%
%% Returns the raw value, or the spec's `default` if the store has no
%% entry, or `undefined` if the key is not registered.
%%
%% == Examples ==
%%
%% ```
%% > arweave_config:get([rocksdb, flush_interval]).
%% 1800
%%
%% > arweave_config:get([does, not, exist]).
%% undefined
%% '''
-spec get(OptionKey) -> Return when
	OptionKey :: [atom() | integer() | binary()],
	Return :: term() | undefined.
get(Option) when is_list(Option) ->
	case arweave_config_options_registry:get(Option) of
		{ok, Value} -> Value;
		_ -> undefined
	end.

%% @doc Set a configuration value using a key.
%%
%% == Examples==
%%
%% ```
%% > set(<<"global.debug">>, <<"true">>).
%% {ok, true}
%%
%% > set([global, debug], true).
%% {ok, true}
%%
%% > set("global.debug", "true").
%% {ok, true}
%%
%% > set("global.debug", 1234).
%% {error, #{ reason => not_boolean }}
%% '''
%%
-spec set(OptionKey, Value) -> Return when
	OptionKey :: atom() | string() | binary() | list(),
	Value :: term(),
	Return :: {ok, term()} | {error, term()}.
set(Key, Value) ->
	case arweave_config_parser:key(Key) of
		{ok, Option} ->
			normalise_set_reply(arweave_config_options_registry:set(Option, Value));
		Else ->
			Else
	end.

%% Collapse the registry's `{ok, NewValue, OldValue}` success shape to
%% the public `{ok, NewValue}` contract.
normalise_set_reply({ok, NewValue, _OldValue}) -> {ok, NewValue};
normalise_set_reply(Other) -> Other.

%% @doc Boot-time bulk mutator. Apply a leaf map emitted by the format
%% parsers to the options registry.
%%
%% Only canonical option_key paths are accepted. Legacy field names
%% are handled by the legacy parsers before values reach this function.
%%
%% Fails fast on the first set that returns an error and reports the
%% offending key. Map iteration order is unspecified, so when more
%% than one entry would fail the specific one surfaced may vary
%% between runs — callers must not rely on a particular failure being
%% reported first.
%%
%% Load-only specs are locked once `runtime/0` has flipped the
%% lifecycle flag; callers that need to mutate config after that point
%% should use `with_test_config/1` (tests only).
-spec load(Map) -> Return when
	Map :: #{[term()] => term()},
	Return :: ok | {error, {[term()], term()}}.
load(Map) when is_map(Map) ->
	maps:fold(
		fun(_, _, {error, _} = Err) -> Err;
		   (Key, Value, ok) ->
				case set(Key, Value) of
					{ok, _} -> ok;
					{ok, _, _} -> ok;
					Else -> {error, {Key, Else}}
				end
		end, ok, Map).

%% @doc Capture the entire config value store as an opaque snapshot for later
%% restoration via `restore/1`. Tests use this to bracket sections that
%% mutate config without leaking changes into sibling tests.
-spec snapshot() -> term().
snapshot() ->
	arweave_config_store:snapshot().

%% @doc Restore a snapshot captured by `snapshot/0`. Drops every
%% current row and re-inserts the snapshot's rows.
-spec restore(term()) -> ok.
restore(Snapshot) ->
	arweave_config_store:restore(Snapshot).

%% @doc Test-only scaffolding. Snapshot the store, run `Fun`, and
%% restore the snapshot on exit (even when `Fun` raises). The runtime
%% guard is temporarily disabled inside `Fun` so tests can mutate
%% load-only specs.
%%
%% Single-threaded: concurrent setters during a `with_test_config/1`
%% call are not safe.
%%
%% Example:
%% ```
%% arweave_config:with_test_config(fun() ->
%%     arweave_config:replace_storage_modules([...]),
%%     %% test body
%% end).
%% '''
-spec with_test_config(fun(() -> Result)) -> Result.
with_test_config(Fun) when is_function(Fun, 0) ->
	StoreSnap = arweave_config_store:snapshot(),
	WasRuntime = is_runtime(),
	case WasRuntime of
		true -> ok = arweave_config_options_registry:set_runtime(false);
		false -> ok
	end,
	try
		Fun()
	after
		arweave_config_store:restore(StoreSnap),
		case WasRuntime of
			true -> ok = arweave_config_options_registry:set_runtime(true);
			false -> ok
		end
	end.

%% @doc Switch to runtime mode. Validators run against the assembled
%% config first; if any rejects, the transition is refused and the
%% system stays in load mode. Once flipped, the transition is
%% one-way.
-spec runtime() -> ok | {error, term()}.
runtime() ->
	case arweave_config_validate:run() of
		ok ->
			arweave_config_options_registry:set_runtime(true);
		{error, _} = Err ->
			Err
	end.

%% @doc Whether arweave_config is in runtime mode.
-spec is_runtime() -> boolean().
is_runtime() ->
	arweave_config_options_registry:is_runtime().

%%====================================================================
%% Public wrappers for indexed namespaces. Callers outside
%% `apps/arweave_config/` must go through these — the delegate
%% submodules are private implementation details.
%%====================================================================

%% @doc Return the list of peers carrying `Role`. `Role` is one of the
%% atoms in `arweave_config_options_peers:roles/0` (or its binary equivalent).
%% Use this for list roles (`trusted`, `block_gossip`, `local`,
%% `cm_peer`, `vdf_client`, `vdf_server`). For singleton roles such
%% as `cm_exit` use `get_peer/1` instead.
-spec get_peers(atom() | binary()) -> [tuple() | binary()].
get_peers(Role) ->
	arweave_config_options_peers:by_role(Role).

%% @doc Return the single peer carrying a singleton `Role` (today only
%% `cm_exit`), or `not_set` if no peer carries it.
-spec get_peer(atom() | binary()) -> tuple() | binary() | not_set.
get_peer(Role) ->
	arweave_config_options_peers:singleton_by_role(Role).

%% @doc Clear all per-peer entries for `Role'. Use before applying a
%% fresh list (or before a `load/1` that supplies a new value) when
%% the store may carry stale entries from an earlier session —
%% per-leaf writes are accumulate-only and can't express "delete
%% everything else first" on their own.
-spec clear_peers(atom()) -> ok | {error, map()}.
clear_peers(Role) when is_atom(Role) ->
	with_runtime_guard(Role, fun() ->
		arweave_config_options_peers:clear_role(Role)
	end).

%% @doc Replace the peers for `Role' wholesale: clear existing entries,
%% then apply the new value. For singleton roles (today only
%% `cm_exit') pass a single peer or `not_set'; for list roles
%% (`trusted', `block_gossip', `local', `cm_peer', `vdf_client',
%% `vdf_server') pass a list of peers.
%%
%% Typical use: a consumer reads its config, runs business logic
%% (validation, filtering, expansion) outside the spec system, then
%% writes the result back.
-spec replace_peers(atom(), Value) -> ok | {error, map()} when
	Value :: tuple() | binary() | not_set | [tuple() | binary()].
replace_peers(Role, Peer) when is_atom(Role), not is_list(Peer) ->
	with_runtime_guard(Role, fun() ->
		arweave_config_options_peers:write_legacy_singleton(Role, Peer)
	end);
replace_peers(Role, Peers) when is_atom(Role), is_list(Peers) ->
	with_runtime_guard(Role, fun() ->
		arweave_config_options_peers:write_legacy_list(Role, Peers)
	end).

%% Reject peer-aggregate mutations once the lifecycle is in runtime
%% mode — per-peer specs default to `runtime => false'.
with_runtime_guard(Role, Fun) ->
	case is_runtime() of
		true ->
			{error, #{
				reason => parameter_not_runtime_writable,
				role => Role
			}};
		false ->
			Fun()
	end.

%% @doc Return the legacy-shaped storage_modules tuple list.
-spec storage_modules() -> [term()].
storage_modules() ->
	arweave_config_options_storage_modules:list().

%% @doc Replace the storage_modules wholesale: clear existing entries
%% then apply the new value. Mirrors `replace_peers/2' for the
%% storage_modules aggregate — per-leaf writes are accumulate-only and
%% can't express "delete everything else first" on their own.
%%
%% Each entry is the legacy `{BucketSize, Bucket, Packing}' tuple;
%% the writer fans the tuple out into `[storage_modules, <id>, ...]'
%% leaves.
-spec replace_storage_modules([term()]) -> ok | {error, map()}.
replace_storage_modules(StorageModules) when is_list(StorageModules) ->
	with_runtime_guard(storage_modules, fun() ->
		arweave_config_options_storage_modules:write_list(StorageModules)
	end).

%% @doc Replace the repack_modules wholesale: clear existing entries
%% then apply the new value. Companion to `replace_storage_modules/1'
%% for the repack-in-place aggregate.
%%
%% Each entry is the legacy `{StorageModule, ToPacking}' tuple; the
%% writer fans the tuple out into `[repack_modules, <id>, ...]' leaves.
-spec replace_repack_modules([term()]) -> ok | {error, map()}.
replace_repack_modules(RepackModules) when is_list(RepackModules) ->
	with_runtime_guard(repack_modules, fun() ->
		arweave_config_options_repack_modules:write_list(RepackModules)
	end).

%% @doc Return the legacy-shaped repack-in-place tuple list.
-spec repack_modules() -> [term()].
repack_modules() ->
	arweave_config_options_repack_modules:list().

%% @doc Return the legacy-shaped defragmentation_modules tuple list.
-spec defrag_modules() -> [term()].
defrag_modules() ->
	arweave_config_options_storage_modules:defrags().

%% @doc Return the configured webhook list. See
%% `arweave_config_options_webhooks:list/0' for the value shape.
-spec webhooks() -> [map()].
webhooks() ->
	arweave_config_options_webhooks:list().

%% @doc Replace the webhooks wholesale: clear existing legacy entries
%% and apply the new value. Each entry is a `#{url, events, headers}'
%% map; the writer synthesizes IDs (`legacy_1', `legacy_2', ...) and
%% fans the map out into `[webhooks, <id>, ...]' leaves.
-spec replace_webhooks([map()]) -> ok | {error, map()}.
replace_webhooks(Webhooks) when is_list(Webhooks) ->
	with_runtime_guard(webhooks, fun() ->
		arweave_config_options_webhooks:write_legacy_list(Webhooks)
	end).

%% @doc Return the per-semaphore concurrency limits as a
%% `#{atom() => pos_integer()}` map. Defaults fill in for unset
%% entries.
-spec semaphores() -> #{atom() => pos_integer()}.
semaphores() ->
	maps:from_list(
		[{Name, Value}
		 || {[semaphores, Name, limit], Value} <-
			get_all_with_prefix([semaphores])]).

%% @doc Return `[{OptionKey, Value}]` for every registered spec whose
%% option_key starts with `Prefix`. Defaults fill in for unset options.
%% Wildcard specs are skipped (see registry's `get_all_with_prefix/1').
-spec get_all_with_prefix(list()) -> [{list(), term()}].
get_all_with_prefix(Prefix) ->
	arweave_config_options_registry:get_all_with_prefix(Prefix).

%% @doc Whether `Flag` is enabled. Reads `[features, Flag]` from the
%% options registry with fallback to the catalog default for the flag.
%% Unknown flags return `false`.
-spec feature_enabled(atom()) -> boolean().
feature_enabled(Flag) ->
	arweave_config_features:enabled(Flag).

%% @doc Log the current configuration to `?LOG_INFO`.
-spec log() -> ok.
log() ->
	arweave_config_store:log().

%% @doc Bootstrap arweave_config from a list of CLI arguments. Loads
%% the environment, parses the config file, parses CLI arguments, and
%% writes the assembled state into the options registry. The `ar` application
%% calls `normalize/0` and `runtime/0` later in its boot sequence.
-spec bootstrap([string() | binary()]) -> ok | {error, term()}.
bootstrap(Args) ->
	arweave_config_bootstrap:start(Args).

%% @doc Normalize the assembled configuration. Promotes legacy
%% enable/disable lists into per-flag `[features, Flag]` entries and
%% performs other post-parse fixups.
-spec normalize() -> ok.
normalize() ->
	arweave_config_normalize:run().

%% @doc Print the command-line help text to standard output.
-spec show_cli_help() -> ok.
show_cli_help() ->
	arweave_config_help:print().

%% @doc Parse a single storage_module configuration string (in the
%% form accepted by the CLI / config file) into the storage_module
%% tuple. Returns `{ok, Tuple}` or `{error, Reason}`.
-spec parse_storage_module(string() | binary()) ->
	{ok, term()} | {error, term()}.
parse_storage_module(Config) ->
	arweave_config_format_legacy_json:parse_storage_module(Config).

%% @doc `application` callback.
start(_StartType, _StartArgs) ->
	?LOG_INFO("arweave_config application starting"),
	arweave_config_sup:start_link().

%% @doc `application` callback.
stop(_Args) ->
	?LOG_INFO("arweave_config application stopped"),
	ok.


%%%===================================================================
%%% Test-only API gated under `-ifdef(AR_TEST)`.
%%%===================================================================

-ifdef(AR_TEST).

%% @doc Apply overrides via `load/1` with the runtime guard
%% temporarily disabled. The flag is snapshotted, flipped to `false`
%% for the duration of the load, then restored (even on raise).
%%
%% Map must contain per-leaf option_keys only (lists of segments).
%% Legacy aggregate shorthands (`storage_modules => [...]`,
%% `{peers, Role} => [...]`, etc.) are NOT accepted — callers should
%% use the dedicated aggregate APIs
%% (`arweave_config:replace_peers/2`,
%% `arweave_config:replace_storage_modules/1`).
%%
%% Use `with_test_config/1` when the store contents must also be
%% snapshotted and restored.
-spec force_config(Map) -> Return when
	Map :: #{[term()] => term()},
	Return :: ok | {error, term()}.
force_config(Map) when is_map(Map) ->
	WasRuntime = is_runtime(),
	case WasRuntime of
		true -> ok = arweave_config_options_registry:set_runtime(false);
		false -> ok
	end,
	try
		load(Map)
	after
		case WasRuntime of
			true -> ok = arweave_config_options_registry:set_runtime(true);
			false -> ok
		end
	end.

%% @doc Test-only: reset the entire config to its post-bootstrap,
%% pre-runtime state. Drops every user-written value (scalars and
%% aggregates alike), then flips the runtime guard back to `false'.
%% After this call, every option returns its spec default.
%%
%% Use this between test cycles in the shared-VM test runner where
%% `arweave_config' survives across tests — without it, both
%% scalar overrides and accumulated aggregate entries (peers,
%% storage_modules, ...) leak from one test into the next.
-spec reset() -> ok.
reset() ->
	ok = restore([]),
	ok = arweave_config_options_registry:set_runtime(false).
-endif.
