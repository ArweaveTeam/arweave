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
%%% == Compatibility and list-backed values ==
%%%
%%% Much of the node still speaks the historical config language.
%%% Specs with a `legacy` field map old atom names to canonical
%%% option_keys. List-backed values (peers, storage modules, webhooks)
%%% are stored under their canonical roots and validated by their specs.
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
	start/0,
	stop/0
]).

%% Public API: webhooks, semaphores, features, limiter
-export([
	feature_enabled/1,
	limiter_groups/0,
	client_throttling_groups/0
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
	parse_storage_module/1,
	storage_module_to_config/1,
	config_to_storage_module/1,
	repack_module_to_config/1,
	config_to_repack_module/1
]).
% application behavior callbacks.
-export([start/2, stop/1]).
-ifdef(AR_TEST).
-export([
	force_config/1,
	restore/1,
	snapshot/0,
	with_test_config/1
]).
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
%% ok
%%
%% > set([global, debug], true).
%% ok
%%
%% > set("global.debug", "true").
%% ok
%%
%% > set("global.debug", 1234).
%% {error, #{ reason => not_boolean }}
%% '''
%%
-spec set(OptionKey, Value) -> Return when
	OptionKey :: atom() | string() | binary() | list(),
	Value :: term(),
	Return :: ok | {error, term()}.
set(Key, Value) ->
	case arweave_config_parser:key(Key) of
		{ok, Option} ->
			case arweave_config_options_registry:set(Option, Value) of
				{ok, _NewValue} -> ok;
				Else -> Else
			end;
		Else ->
			Else
	end.

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
					ok -> ok;
					Else -> {error, {Key, Else}}
				end
		end, ok, Map).

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

%% @doc Return the list of rate-limiter group IDs used by
%% `arweave_limiter_sup` to build one supervisor branch per group.
%% Per-field values for a given group are read via
%% `arweave_config:get([limiter, GroupID, Field])'.
-spec limiter_groups() -> [atom()].
limiter_groups() ->
	arweave_config_options_limiter:group_ids().

%% @doc Return the list of client throttling group IDs used by
%% `arweave_limiter_sup` to build one supervisor branch per group.
%% Per-field values for a given group are read via
%% `arweave_config:get([limiter, GroupID, Field])'.
-spec client_throttling_groups() -> [atom()].
client_throttling_groups() ->
	arweave_config_options_client_throttling:group_ids().

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

-spec storage_module_to_config(map() | {pos_integer(), non_neg_integer(), term()}) ->
	map().
storage_module_to_config(StorageModule) ->
	arweave_config_options_storage_modules:storage_module_to_config(StorageModule).

-spec config_to_storage_module(map()) -> {pos_integer(), non_neg_integer(), term()}.
config_to_storage_module(Config) ->
	arweave_config_options_storage_modules:config_to_storage_module(Config).

-spec repack_module_to_config({{pos_integer(), non_neg_integer(), term()}, term()}) -> map().
repack_module_to_config(RepackModule) ->
	arweave_config_options_repack_modules:repack_module_to_config(RepackModule).

-spec config_to_repack_module(map()) ->
	{{pos_integer(), non_neg_integer(), term()}, term()}.
config_to_repack_module(Config) ->
	arweave_config_options_repack_modules:config_to_repack_module(Config).

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

%% @doc Capture the store and runtime flag as an opaque snapshot for
%% restoration via `restore/1`, so tests can mutate config without
%% leaking into siblings.
-spec snapshot() -> #{store := list(), runtime := boolean()}.
snapshot() ->
	#{
		store => arweave_config_store:snapshot(),
		runtime => is_runtime()
	}.

%% @doc Restore a `snapshot/0`: replace every store row with the
%% snapshot's rows and restore the captured runtime flag.
-spec restore(#{store := list(), runtime := boolean()}) -> ok.
restore(#{store := StoreSnapshot, runtime := Runtime}) when is_boolean(Runtime) ->
	ok = arweave_config_store:restore(StoreSnapshot),
	ok = arweave_config_options_registry:set_runtime(Runtime).

%% @doc Test-only scaffolding. Snapshot the store, run `Fun`, and
%% restore the snapshot on exit (even when `Fun` raises).
%%
%% Single-threaded: concurrent setters during a `with_test_config/1`
%% call are not safe.
%%
%% Example:
%% ```
%% arweave_config:with_test_config(fun() ->
%%     ok = arweave_config:force_config(#{[storage_modules] => [...]}),
%%     %% test body
%% end).
%% '''
-spec with_test_config(fun(() -> Result)) -> Result.
with_test_config(Fun) when is_function(Fun, 0) ->
	Snapshot = snapshot(),
	try
		Fun()
	after
		restore(Snapshot)
	end.

%% @doc Apply overrides via `load/1` with the runtime guard
%% temporarily disabled. The flag is snapshotted, flipped to `false`
%% for the duration of the load, then restored (even on raise).
%%
%% Map must contain per-leaf option_keys only (lists of segments).
%% Legacy list shorthands (`storage_modules => [...]`,
%% `{peers, Role} => [...]`, etc.) are NOT accepted — callers should
%% use canonical option paths such as `[peers, trusted]`.
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

-endif.
