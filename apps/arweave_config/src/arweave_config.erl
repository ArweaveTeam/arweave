%%% @doc Public facade and lifecycle coordinator for Arweave configuration.
%%%
%%% The `arweave_config` app owns the node configuration. Most callers
%%% should treat this module as the public boundary.
%%%
%%% == Process topology ==
%%%
%%% Starting the application brings up a one-for-all supervisor with
%%% four long-lived pieces:
%%% - this facade process (arweave_config)
%%% - the value store (arweave_config_store)
%%% - the option-spec registry (arweave_config_options_registry)
%%% - the signal handler (arweave_config_signal_handler)
%%% 
%%% This module's gen_server owns only the load/runtime lifecycle flag.
%%% The registry owns spec lookup and mutation semantics, and the store
%%% owns the actual values.
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
-behavior(gen_server).
-export([
	get/1,
	get/2,
	get_all_with_prefix/1,
	is_runtime/0,
	runtime/0,
	set/2,
	load/1,
	with_test_config/1,
	snapshot/0,
	restore/1,
	start/0,
	start_link/0,
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
	parse_storage_modules/1
]).
%% Public API: webhooks, semaphores, features
-export([
	webhooks/0,
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
% gen_server behavior callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2]).
-ifdef(AR_TEST).
-export([force_config/1]).
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

%% @doc Get a value from the configuration.
%%
%% Accepts canonical new-style option_key spellings:
%%
%% ```
%% > get('rocksdb.flush_interval').
%% {ok, 1800}
%%
%% > get(<<"rocksdb.flush_interval">>).
%% {ok, 1800}
%%
%% > get([rocksdb, flush_interval]).
%% {ok, 1800}
%%
%% > get([test]).
%% {error, #{ reason => not_found }}.
%% '''
%%
%% Defaults declared in the option spec are returned automatically
%% when the underlying store has no value, so callers don't need to
%% pass a default.
%%
%% Returns the raw value, or `undefined` when the option is unknown
%% or has no value set.
-spec get(OptionKey) -> Return when
	OptionKey :: atom() | string() | binary() | list(),
	Return :: term() | undefined.
get(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Option} ->
			spec_get(Option);
		_ ->
			undefined
	end.

%% Unwrap `{ok, Value} | {error, _}` into `Value | undefined`.
spec_get(Option) ->
	case arweave_config_options_registry:get(Option) of
		{ok, Value} -> Value;
		_ -> undefined
	end.

%% @doc Get a value from the configuration; return `Default` if not
%% set.
%%
%% == Examples ==
%%
%% ```
%% > get(<<"global.debug">>, true).
%% false
%%
%% > get([global, debug], true).
%% false
%%
%% > get([test], true).
%% true
%% '''
-spec get(OptionKey, Default) -> Return when
	OptionKey :: atom() | string() | binary() | list(),
	Default :: term(),
	Return :: term().
get(Key, Default) ->
	case get(Key) of
		undefined -> Default;
		Value -> Value
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
%%     arweave_config:load(arweave_config:parse_storage_modules([...])),
%%     %% test body
%% end).
%% '''
-spec with_test_config(fun(() -> Result)) -> Result.
with_test_config(Fun) when is_function(Fun, 0) ->
	StoreSnap = arweave_config_store:snapshot(),
	WasRuntime = is_runtime(),
	case WasRuntime of
		true -> _ = gen_server:call(?MODULE, {set_runtime, false}, 1000);
		false -> ok
	end,
	try
		Fun()
	after
		arweave_config_store:restore(StoreSnap),
		case WasRuntime of
			true -> _ = gen_server:call(?MODULE, {set_runtime, true}, 1000);
			false -> ok
		end
	end.

%% @doc Start arweave_config process.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Switch to runtime mode. Validators run against the assembled
%% config first; if any rejects, the transition is refused and the
%% system stays in load mode. Once flipped, the transition is
%% one-way.
-spec runtime() -> ok | {error, term()}.
runtime() ->
	case arweave_config_validate:run() of
		ok ->
			gen_server:call(?MODULE, runtime, 10_000);
		{error, _} = Err ->
			Err
	end.

%% @doc Whether arweave_config is in runtime mode.
-spec is_runtime() -> boolean().
is_runtime() ->
	case ets:lookup(?MODULE, runtime) of
		[{runtime, true}] -> true;
		_Else -> false
	end.

%% @doc `gen_server` callback.
init(_) ->
	ets:new(?MODULE, [named_table, protected]),
	{ok, ?MODULE}.

%% @doc `gen_server` callback.
terminate(_, _) ->
	?LOG_INFO("arweave_config process stopped").

%% @doc `gen_server` callback.
handle_call(runtime, _From, State) ->
	try
		ets:insert(?MODULE, {runtime, true})
	of
		true -> ok;
		_ -> ok
	catch
		_:_ -> ok
	end,
	{reply, ok, State};
handle_call({set_runtime, Bool}, _From, State) when is_boolean(Bool) ->
	try
		ets:insert(?MODULE, {runtime, Bool})
	of
		true -> ok;
		_ -> ok
	catch
		_:_ -> ok
	end,
	{reply, ok, State};
handle_call(_, _, State) -> {noreply, State}.

%% @doc `gen_server` callback.
handle_cast(_, State) ->
	{noreply, State}.

%% @doc `gen_server` callback.
handle_info(_, State) -> {noreply, State}.

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

%% @doc Return the legacy-shaped repack-in-place tuple list.
-spec repack_modules() -> [term()].
repack_modules() ->
	arweave_config_options_repack_modules:list().

%% @doc Return the legacy-shaped defragmentation_modules tuple list.
-spec defrag_modules() -> [term()].
defrag_modules() ->
	arweave_config_options_storage_modules:defrags().

%% @doc Convert a list of legacy storage_module tuples into a flat
%% per-leaf entry map suitable for merging into an
%% `arweave_config:load/1` map. Each tuple expands to entries under
%% `[storage_modules, <id>, ...]` covering range/partition + packing
%% attributes.
%%
%% Example:
%% ```
%% arweave_config:load(maps:merge(
%%     arweave_config:parse_storage_modules(StorageModules),
%%     #{ [data_dir] => DataDir, [mining, address] => Addr })).
%% '''
-spec parse_storage_modules([term()]) -> #{[term()] => term()}.
parse_storage_modules(StorageModules) ->
	arweave_config_options_storage_modules:to_entry_map(StorageModules).

%% @doc Return the configured webhook list. See
%% `arweave_config_options_webhooks:list/0' for the value shape.
-spec webhooks() -> [map()].
webhooks() ->
	arweave_config_options_webhooks:list().

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
%% Returns whatever `load/1` returns.
%%
%% Use `with_test_config/1` when the store contents must also be
%% snapshotted and restored.
-spec force_config(Map) -> Return when
	Map :: #{[term()] => term()},
	Return :: ok | {error, [{[term()], term()}]}.
force_config(Map) when is_map(Map) ->
	WasRuntime = is_runtime(),
	case WasRuntime of
		true -> ok = set_runtime_local(false);
		false -> ok
	end,
	try
		load(Map)
	after
		case WasRuntime of
			true -> ok = set_runtime_local(true);
			false -> ok
		end
	end.

set_runtime_local(Bool) when is_boolean(Bool) ->
	_ = gen_server:call(?MODULE, {set_runtime, Bool}, 1000),
	ok.
-endif.
