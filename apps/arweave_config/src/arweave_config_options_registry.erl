%%% @doc Runtime registry for arweave option specifications.
%%%
%%% Owns the gen_server + ETS-backed options registry and the read/write
%%% pipeline. The pure spec assembly and normalization live in
%%% `arweave_config_options_spec`.
%%%
%%% Acts as a frontend for `arweave_config_store`: options are
%%% checked against their specs, then valid writes are forwarded to
%%% the store.
%%%
-module(arweave_config_options_registry).
-behavior(gen_server).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/1]}).
-export([start_link/0,
         start_link/1,
         stop/0,
         resolve/1,
         default/1,
         is_default/2,
         list_item_key/2,
         type/1,
         get_legacy/0,
         get_legacy/1,
         get_environments/0,
         get_long_arguments/0,
         get/1,
         get_all_with_prefix/1,
         set/2,
         set_local/2,
         is_runtime/0,
         set_runtime/1
        ]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-ifdef(AR_TEST).
-export([get_long_argument/1]).
-endif.
-include_lib("kernel/include/logger.hrl").

%% Dedicated ETS table that holds the load-vs-runtime lifecycle flag.
%% Kept separate from the spec table so the schemas don't mix.
-define(RUNTIME_TABLE, arweave_config_options_registry_runtime).

%% Process-dictionary key backing `with_value_under_validation/3' and
%% `value_under_validation/1'. The process dictionary is private to a
%% single process — that privacy is the whole point (see those functions).
-define(VALIDATION_IN_PROGRESS, '$arweave_config_validation_in_progress').

%% @doc Start the registry process with the default spec set.
-spec start_link() -> Return when
    Return :: {ok, pid()}.
start_link() ->
    start_link([]).

%% @doc Start the registry process with an explicit spec list.
-spec start_link(Specs) -> Return when
    Specs :: [map() | atom()],
    Return :: {ok, pid()}.
start_link(Specs) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Specs, []).

%% @doc Stop the registry process.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Look up an option's specification. The registry table is a `set'
%% keyed on the option_key, so this is a direct key lookup rather than a
%% match-spec scan — the common read path stays a couple of ETS lookups.
spec(ParameterSpec) ->
    case ets:lookup(?MODULE, ParameterSpec) of
        [{Option, Spec}] ->
            {ok, Option, Spec};
        _Else ->
            {error, not_found}
    end.

%% @doc Resolve a runtime option to its exact spec.
-spec resolve(Option) -> Return when
    Option :: list(),
    Return :: {ok, list(), map(), map()}
        | {error, term()}.
resolve(Option) ->
    case spec(Option) of
        {ok, _, Spec} ->
            {ok, Option, Spec, #{}};
        _ ->
            {error, not_found}
    end.

%% @doc The spec's declared default for the option at `Option', or
%% `error' when the option has no spec or its spec declares no default.
-spec default(Option) -> Return when
    Option :: list(),
    Return :: {ok, term()} | error.
default(Option) ->
    find_in_spec(Option, default).

%% @doc Whether `Value' equals the spec's declared default for the
%% option at `Option'. `false' when the option has no spec or its spec
%% declares no default.
-spec is_default(Option, Value) -> boolean() when
    Option :: list(),
    Value :: term().
is_default(Option, Value) ->
    default(Option) =:= {ok, Value}.

%% @doc The canonical spec key for `Field' inside `Root''s list items
%% (a `list_map' option such as `[storage_modules]' or `[webhooks]').
-spec list_item_key(Root, Field) -> Key when
    Root :: list(),
    Field :: atom(),
    Key :: list().
list_item_key(Root, Field) when is_list(Root), is_atom(Field) ->
    Root ++ [{list_item}, Field].

%% @doc The spec's registered type for the option at `Option', or
%% `error' when the option has no spec or its spec declares no type.
-spec type(Option) -> Return when
    Option :: list(),
    Return :: {ok, atom()} | error.
type(Option) ->
    find_in_spec(Option, type).

find_in_spec(Option, SpecKey) ->
    case resolve(Option) of
        {ok, _Option, Spec, _Bindings} -> maps:find(SpecKey, Spec);
        _ -> error
    end.

%% @doc List of supported environment variables.
get_environments() ->
    Pattern = {'$1', #{ environment => '$2'}},
    Guard = [],
    Select = [{{'$2', '$1'}}],
    ets:select(?MODULE, [{Pattern, Guard, Select}]).

%% @doc List of supported long arguments with their nargs spec.
get_long_arguments() ->
    Pattern = {'$1', #{ long_argument => '$2' }},
    Guard = [],
    Select = [{{'$2', '$_'}}],
    % match spec does not support correctly map, so, a filter
    % is required to cleanup things.
    [
        {Argument, Spec}
        || {Argument, {_, Spec}}
        <- ets:select(?MODULE, [{Pattern, Guard, Select}])
    ].

%% @doc Map of every spec's legacy field name to its option_key.
get_legacy() ->
    Pattern = {'$1', #{ legacy => '$2' }},
    Guard = [{'=/=', '$2', undefined}],
    Select = [{{'$2', '$1'}}],
    Query = [{Pattern, Guard, Select}],
    maps:from_list(ets:select(?MODULE, Query)).

%% @doc Look up the option_key for a legacy field name.
get_legacy(Key) ->
    Pattern = {'$1', #{ legacy => Key }},
    Guard = [{'=/=', Key, undefined}],
    Select = ['$1'],
    Query = [{Pattern, Guard, Select}],
    case ets:select(?MODULE, Query) of
        [V] ->
            {ok, V};
        _ ->
            {error, undefined}
    end.

%% @doc Every live config entry whose key starts with `Prefix',
%% drawing on both the registry's specs and runtime-only store entries.
-spec get_all_with_prefix(list()) -> [{list(), term()}].
get_all_with_prefix(Prefix) ->
    %% Return `[]` when the registry's ETS table is gone — that happens
    %% transiently during app shutdown if a late-arriving gen_server
    %% cast (e.g., `ar_poller`'s periodic `collect_peers') races with
    %% `application:stop(arweave_config)' from `ar:stop_dependencies/0'.
    %% Without this guard the cast crashes and takes its supervisor
    %% subtree with it.
    case ets:info(?MODULE, name) of
        undefined -> [];
        _ ->
            ConcreteSpecEntries = [
                {Key, V}
                || {Key, _Spec} <- ets:tab2list(?MODULE),
                   is_list(Key),
                   lists:prefix(Prefix, Key),
                   not is_schema_key(Key),
                   {ok, V} <- [?MODULE:get(Key)]
            ],
            StoreEntries = arweave_config_store:items_with_prefix(Prefix),
            ConcreteKeys = sets:from_list([K || {K, _} <- ConcreteSpecEntries]),
            InstanceEntries = [
                E || {K, _} = E <- StoreEntries,
                     not sets:is_element(K, ConcreteKeys)
            ],
            ConcreteSpecEntries ++ InstanceEntries
    end.

is_schema_key(Key) ->
    lists:any(fun({_}) -> true; (_) -> false end, Key).

%% @doc Read a value. This is a direct read: both the spec table and the
%% value store are `protected' ETS, so reads run in the caller's process
%% and never hit the gen_server — fast and fully concurrent. The one
%% exception is the process running a runtime `set''s validators: it sees
%% that set's not-yet-committed value via `value_under_validation/1' (so
%% the validators check the new value), whereas every other process reads
%% the committed store and can never observe an unvalidated value.
get(Option) ->
    case value_under_validation(Option) of
        {ok, _} = Found -> Found;
        none -> do_get(Option)
    end.

%% @doc Run `Fun' with `Option => Value' visible to configuration reads
%% made *by the current process only*. A runtime `set' uses this so its
%% validators — which run synchronously in this same process — observe the
%% value being validated, while every other process keeps reading the
%% committed store. It is backed by the process dictionary precisely
%% because that is private per process, so the in-flight value cannot leak
%% to a concurrent reader. The entry is cleared on exit.
-spec with_value_under_validation(Option, Value, Fun) -> Result when
    Option :: list(),
    Value :: term(),
    Fun :: fun(() -> Result),
    Result :: term().
with_value_under_validation(Option, Value, Fun) ->
    erlang:put(?VALIDATION_IN_PROGRESS, #{Option => Value}),
    try
        Fun()
    after
        erlang:erase(?VALIDATION_IN_PROGRESS)
    end.

%% @doc The value the *current* process is mid-validating for `Option', or
%% `none'. Only `with_value_under_validation/3' ever sets this, and only
%% around the validator pass, so every other process — and this one
%% outside a `set' — gets `none'.
-spec value_under_validation(list()) -> {ok, term()} | none.
value_under_validation(Option) ->
    case erlang:get(?VALIDATION_IN_PROGRESS) of
        #{Option := Value} -> {ok, Value};
        _ -> none
    end.

%% @doc Set a value via the registry. Validates the key + value, runs
%% any `handle_set` side effect, and stores the value in
%% `arweave_config_store`.
%%
%% == Examples ==
%%
%% ```
%% {ok, NewValue = true} = set([global, debug], <<"true">>).
%% '''
-spec set(Option, Value) -> Return when
    Option :: [atom() | iolist()],
    Value :: term(),
    Return :: {ok, term()} | {error, term()}.
set(Option, Value) ->
    %% Same self-call protection as `get/1`.
    case whereis(?MODULE) of
        Self when Self =:= self() ->
            set_local(Option, Value);
        _ ->
            gen_server:call(?MODULE, {set, Option, Value}, 10_000)
    end.

%% @doc Set an option without going through the gen_server. Safe
%% from inside a `handle_set/4` callback (which is already on the
%% gen_server process), or from any context that can guarantee no
%% concurrent sets.
-spec set_local(Option, Value) -> Return when
    Option :: list(),
    Value :: term(),
    Return :: {ok, term()} | {error, term()}.
set_local(Option, Value) ->
    do_set(Option, Value).

%% @doc Whether the registry is in runtime mode. Lock-free ETS read so
%% callers (including the registry process itself) can check without
%% deadlocking through the gen_server.
-spec is_runtime() -> boolean().
is_runtime() ->
    case ets:lookup(?RUNTIME_TABLE, runtime) of
        [{runtime, true}] -> true;
        _ -> false
    end.

%% @doc Set the lifecycle flag. The facade flips it one-way at
%% `arweave_config:runtime/0`; tests flip it both directions.
-spec set_runtime(boolean()) -> ok.
set_runtime(Bool) when is_boolean(Bool) ->
    _ = gen_server:call(?MODULE, {set_runtime, Bool}, 10_000),
    ok.

-spec init(Specs) -> Return when
    Specs :: [atom() | map()],
    Return :: {ok, NamedETS},
    NamedETS :: ?MODULE.
init([]) ->
    Specs = arweave_config_options_spec:all(),
    init_process(Specs);
init(Specs) when is_list(Specs) ->
    init_process(Specs).

%% Init runs with trap_exit so a contributor crash surfaces as a
%% gen_server termination reason rather than killing the supervisor
%% tree silently.
init_process(Specs) ->
    erlang:process_flag(trap_exit, true),
    init_ets(Specs).

%% The registry ETS table is `protected` so only this gen_server can write
%% to it at runtime.
init_ets(Specs) ->
    ets:new(?MODULE, [
        named_table,
        protected
    ]),
    ets:new(?RUNTIME_TABLE, [
        named_table,
        protected
    ]),
    %% Boot in load mode; `arweave_config:runtime/0` flips this later.
    ets:insert(?RUNTIME_TABLE, {runtime, false}),
    case arweave_config_options_spec:normalize_specs(Specs) of
        {ok, MapSpec} ->
            init_state(MapSpec);
        {error, Reason} ->
            {stop, Reason}
    end.

%% Load every normalized spec into the ETS registry.
init_state(MapSpec) ->
    [
        ets:insert(?MODULE, {K, V})
        ||
        {K, V} <- maps:to_list(MapSpec)
    ],
    init_final(?MODULE).

init_final(State) ->
    ?LOG_INFO("~p ready", [?MODULE]),
    {ok, State}.

terminate(_, _) ->
    ok.

handle_call({set, Option, Value}, _From, State) ->
    {reply, do_set(Option, Value), State};
handle_call({set_runtime, Bool}, _From, State) when is_boolean(Bool) ->
    ets:insert(?RUNTIME_TABLE, {runtime, Bool}),
    {reply, ok, State};
handle_call(Msg, From, State) ->
    ?LOG_WARNING([
        {message, Msg},
        {from, From},
        {module, ?MODULE},
        {function, handle_call}
    ]),
    {reply, {error, {unknown_call, Msg}}, State}.

handle_cast(Msg, State) ->
    ?LOG_WARNING([
        {message, Msg},
        {module, ?MODULE},
        {function, handle_cast}
    ]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING([
        {message, Msg},
        {module, ?MODULE},
        {function, handle_info}
    ]),
    {noreply, State}.

%% Type-check a value against its spec.
check(Option, Value, Spec) ->
    check_type(Option, Value, Spec, #{}).

%% Dispatch to the type function in `arweave_config_type`. Expected
%% return is `ok`, `{ok, ConvertedValue}`, or `{error, Term}`.
check_type(Option, Value, Spec = #{ type := list_map }, Buffer) ->
    check_list_map_type(Option, Value, Value, Spec, Buffer);
%% The `storage_modules` / `repack_modules` types are list_map
%% variants that additionally accept runtime tuples as entries: the
%% type function converts tuples to their canonical maps, then the
%% usual `{list_item}` schema check runs (atomizing keys and coercing
%% field values). A value the type function rejects is passed through
%% unconverted so `check_list_map' reports its usual error.
check_type(Option, Value, Spec = #{ type := Type }, Buffer)
        when Type =:= storage_modules;
             Type =:= repack_modules ->
    Normalized = case arweave_config_type:Type(Value) of
        {ok, V} -> V;
        {error, _} -> Value
    end,
    check_list_map_type(Option, Normalized, Value, Spec, Buffer);
check_type(Option, Value, Spec = #{ type := Type }, Buffer) ->
    case
        check_type(Value, Type)
    of
        ok ->
            NewBuffer = Buffer#{ type => ok },
            check_final(Option, Value, Spec, NewBuffer);
        {ok, V} ->
            NewBuffer = Buffer#{ type => ok },
            check_final(Option, V, Spec, NewBuffer);
        Error ->
            NewBuffer = Buffer#{ type => Error },
            check_final(Option, Value, Spec, NewBuffer)
    end;
check_type(Option, Value, Spec, Buffer) ->
    check_final(Option, Value, Spec, Buffer#{ type => undefined }).

-spec check_type(Value, Types) -> Return when
    Value :: term(),
    Types :: atom() | [atom()],
    Return :: {ok, Value} | {error, term()}.
check_type(Value, Types) when is_list(Types) ->
    check_types(Value, Types);
check_type(Value, Type) when is_atom(Type) ->
    try
        arweave_config_type:Type(Value)
    of
        ok -> {ok, Value};
        {ok, V} -> {ok, V};
        {error, Reason} -> {error, Reason}
    catch
        E:R -> {error, {E, R}}
    end.

%% Try each type in order, returning the first one that accepts the value.
-spec check_types(Value, Types) -> Return when
    Value :: term(),
    Types :: [atom()],
    Return :: {ok, Value} | {error, term()}.
check_types(_Value, []) ->
    {error, <<"value does not match types">>};
check_types(Value, [Type|Rest]) when is_atom(Type) ->
        case check_type(Value, Type) of
            ok ->
                {ok, Value};
            {ok, V} ->
                {ok, V};
            {error, _} ->
                check_types(Value, Rest)
        end.

check_final(Option, Value, _, Buffer) ->
    ?LOG_DEBUG("~p", [Buffer]),
    case Buffer of
        #{ type := undefined } ->
            {ok, Value, Buffer};
        #{ type := ok } ->
            {ok, Value, Buffer};
        _ ->
            {error, #{
                option => Option,
                reason => type_check_failed,
                value => Value,
                details => Buffer
            }}
    end.

%% Resolve the spec then dispatch through the runtime-writability
%% guard and the validation pipeline.
do_set(Option, Value) ->
    case resolve(Option) of
        {ok, Option, Spec, Bindings} ->
            case is_list_item_schema_key(Option) of
                true ->
                    {error, #{
                        option => Option,
                        reason => not_canonical_option
                    }};
                false ->
                    do_set_runtime(Option, Value, Spec, Bindings)
            end;
        Else ->
            Else
    end.

%% Enforce the runtime-writability guard. During load every spec
%% accepts sets; once in runtime mode only `runtime => true` specs do.
do_set_runtime(Option, Value, Spec, Bindings) ->
    RuntimeWritable = maps:get(runtime, Spec, false),
    InRuntime = is_runtime(),
    case {InRuntime, RuntimeWritable} of
        {false, _} ->
            do_set_parameter(Option, Value, Spec, Bindings);
        {true, true} ->
            do_set_parameter(Option, Value, Spec, Bindings);
        {true, false} ->
            {error, #{
                    option => Option,
                    reason => parameter_not_runtime_writable,
                    value => Value
                }
            }
    end.

do_set_parameter(Option, Value, Spec, Bindings) ->
    %% Writing a value equal to the spec's `default' bypasses the type
    %% validator, so callers can reset to a sentinel default (e.g.
    %% `not_set') that no general-purpose type function would accept.
    case maps:get(default, Spec, undefined) of
        Value ->
            do_set_value(Option, Value, Spec, Bindings);
        _ ->
            case check(Option, Value, Spec) of
                {ok, Return, _} ->
                    do_set_value(Option, Return, Spec, Bindings);
                Else ->
                    Else
            end
    end.

%% Run the `handle_set` callback when one is defined; otherwise fall
%% through to the plain-store branch below.
do_set_value(Option, Value, Spec = #{ set := Set }, Bindings) ->
    Default = maps:get(default, Spec, undefined),
    OldValue = arweave_config_store:get(Option, Default),
    State = local_state(#{
        spec => Spec,
        old_value => OldValue,
        bindings => Bindings
    }),

    Args = maps:get(set_args, Spec, []),
    try
        Set(Option, Value, State, Args)
    of
        ignore ->
            {ok, OldValue};
        {ok, NewValue} ->
            {ok, NewValue};
        {store, NewValue} ->
            do_set_store_with_validation(
                Option, NewValue, OldValue, Spec);
        {error, _} = Err ->
            %% Surface a callback rejection (e.g. bad_mining_address)
            %% without crashing the gen_server.
            Err
    catch
        E:R ->
            {error, {E, R}}
    end;
do_set_value(Option, Value, Spec, _Bindings) ->
    Default = maps:get(default, Spec, undefined),
    OldValue = arweave_config_store:get(Option, Default),
    do_set_store_with_validation(Option, Value, OldValue, Spec).

%% Validate before committing. In runtime mode, run the validators with
%% the new value exposed only to this process (via
%% `with_value_under_validation/3', so the validators see it but no other
%% process does) and write to the store only if the assembled config stays
%% valid. A rejected set therefore never touches the committed store —
%% concurrent direct-ETS reads can only ever observe a validated value,
%% so no transient/rolled-back window exists. During load mode validation
%% is deferred to `arweave_config:runtime/0', so the write is committed
%% directly.
do_set_store_with_validation(Option, NewValue, OldValue, Spec) ->
    case is_runtime() of
        true ->
            with_value_under_validation(Option, NewValue, fun() ->
                case arweave_config_validate:run() of
                    ok -> do_set_store(Option, NewValue, OldValue, Spec);
                    {error, _} = Err -> Err
                end
            end);
        false ->
            do_set_store(Option, NewValue, OldValue, Spec)
    end.

do_set_store(Option, NewValue, _OldValue, _Spec) ->
    try arweave_config_store:set(Option, NewValue) of
        {ok, {_, _}} ->
            {ok, NewValue};
        Else ->
            Else
    catch
        E:R ->
            {error, {E, R}}
    end.

do_get(Option) ->
    case resolve(Option) of
        {ok, Option, Spec, _Bindings} ->
            case is_list_item_schema_key(Option) of
                true -> {error, not_canonical_option};
                false -> do_get2(Option, Spec)
            end;
        Else ->
            Else
    end.

do_get2(Option, Spec = #{ get := Get, default := Default }) ->
    State = local_state(#{ spec => Spec }),
    case Get(Option, State) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            {ok, Default}
    end;
do_get2(Option, Spec = #{ get := Get }) ->
    State = local_state(#{ spec => Spec }),
    case Get(Option, State) of
        {ok, Value} ->
            {ok, Value};
        Else ->
            {error, Else}
    end;
do_get2(Option, _Spec = #{ default := Default }) ->
    case arweave_config_store:get(Option) of
        {ok, Value} ->
            {ok, Value};
        _ ->
            {ok, Default}
    end;
do_get2(Option, _Spec) ->
    arweave_config_store:get(Option).

strip_prefix([], Rest) ->
    Rest;
strip_prefix([H | Prefix], [H | Key]) ->
    strip_prefix(Prefix, Key);
strip_prefix(_Prefix, _Key) ->
    false.

is_list_item_schema_key(Key) ->
    lists:member({list_item}, Key).

%% Validate a list_map-shaped value against its `{list_item}` schema.
%% On success the checked (normalized) list is stored; on error the
%% caller's original value is reported.
check_list_map_type(Option, Value, OriginalValue, Spec, Buffer) ->
    case check_list_map(Option, Value) of
        {ok, V} ->
            check_final(Option, V, Spec, Buffer#{ type => ok });
        Error ->
            check_final(Option, OriginalValue, Spec, Buffer#{ type => Error })
    end.

check_list_map(Option, Values) when is_list(Values) ->
    FieldSpecs = list_item_field_specs(Option),
    case FieldSpecs of
        [] ->
            {error, no_list_item_specs};
        _ ->
            check_list_map_items(Option, Values, maps:from_list(FieldSpecs), [])
    end;
check_list_map(_Option, Value) ->
    {error, Value}.

list_item_field_specs(Prefix) ->
    [
        {Field, Spec}
        || {Key, Spec} <- ets:tab2list(?MODULE),
           {list_item_field, Field} <- [list_item_field(Prefix, Key)]
    ].

list_item_field(Prefix, Key) when is_list(Key) ->
    case strip_prefix(Prefix, Key) of
        [{list_item}, Field] when is_atom(Field) ->
            {list_item_field, Field};
        _ ->
            false
    end;
list_item_field(_Prefix, _Key) ->
    false.

check_list_map_items(_Option, [], _FieldSpecs, Acc) ->
    {ok, lists:reverse(Acc)};
check_list_map_items(Option, [Item | Rest], FieldSpecs, Acc)
        when is_map(Item) ->
    case check_list_map_item(Option, Item, FieldSpecs) of
        {ok, Checked} ->
            check_list_map_items(Option, Rest, FieldSpecs, [Checked | Acc]);
        {error, _} = Err ->
            Err
    end;
check_list_map_items(_Option, [Item | _Rest], _FieldSpecs, _Acc) ->
    {error, #{ reason => item_not_map, item => Item }}.

check_list_map_item(Option, Item, FieldSpecs) ->
    NormalizedItem = normalize_list_map_item_keys(Item),
    case unknown_list_map_fields(NormalizedItem, FieldSpecs) of
        [] ->
            check_list_map_fields(
                Option, maps:to_list(FieldSpecs), NormalizedItem, #{});
        Unknown ->
            {error, #{ reason => unknown_fields, fields => Unknown }}
    end.

normalize_list_map_item_keys(Item) ->
    maps:from_list([
        {arweave_config_leaf_map:convert_key(Key), Value}
        || {Key, Value} <- maps:to_list(Item)
    ]).

unknown_list_map_fields(Item, FieldSpecs) ->
    [Key || Key <- maps:keys(Item), not maps:is_key(Key, FieldSpecs)].

check_list_map_fields(_Option, [], _Item, Acc) ->
    {ok, Acc};
check_list_map_fields(Option, [{Field, Spec} | Rest], Item, Acc) ->
    case maps:find(Field, Item) of
        {ok, Value} ->
            check_list_map_field(Option, Field, Value, Spec, Rest, Item, Acc);
        error ->
            case maps:find(default, Spec) of
                {ok, Default} ->
                    check_list_map_field(
                        Option, Field, Default, Spec, Rest, Item, Acc);
                error ->
                    check_list_map_fields(Option, Rest, Item, Acc)
            end
    end.

check_list_map_field(Option, Field, Value, Spec, Rest, Item, Acc) ->
    FieldOption = list_item_key(Option, Field),
    case check(FieldOption, Value, Spec) of
        {ok, Checked, _} ->
            check_list_map_fields(
              Option, Rest, Item, Acc#{ Field => Checked });
        {error, Reason} ->
            {error, #{
                      reason => field_check_failed,
                      field => Field,
                      value => Value,
                      details => Reason
                     }}
    end.

local_state(Map) ->
    maps:merge(Map, #{config => arweave_config_store:to_map()}).

%%%===================================================================
%%% Test-only API gated under `-ifdef(AR_TEST)`.
%%%===================================================================

-ifdef(AR_TEST).

%% @doc Look up a spec by its long argument.
get_long_argument(ArgumentKey) ->
    Pattern = {'$1', #{ long_argument => '$2' }},
    Guard = [{'=:=', '$2', ArgumentKey}],
    Select = [{{'$2', '$_'}}],
    % match spec does not support correctly map, so, a filter
    % is required to cleanup things.
    [
        {Argument, Spec}
        || {Argument, {_, Spec}}
        <- ets:select(?MODULE, [{Pattern, Guard, Select}])
    ].
-endif.
