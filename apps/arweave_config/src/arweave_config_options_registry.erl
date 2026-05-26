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
-export([
	start_link/0,
	start_link/1,
	stop/0,
	resolve/1,
	get_legacy/0,
	get_legacy/1,
	get_environments/0,
	get_long_arguments/0,
	get/1,
	get_local/1,
	get_all_with_prefix/1,
	set/2,
	set_local/2
]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-ifdef(AR_TEST).
-export([get_long_argument/1]).
-endif.
-include_lib("kernel/include/logger.hrl").

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

%% @doc Look up an option's specification.
spec(ParameterSpec) ->
	Pattern = {'$1', '$2'},
	Guard = [{'=:=', '$1', ParameterSpec}],
	Select = [{{'$1', '$2'}}],
	case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
		[{Option, Spec}] ->
			{ok, Option, Spec};
		_Else ->
			{error, not_found}
	end.

%% @doc Resolve a runtime option to a spec, with `{Variable}`
%% wildcard segments matched against concrete values. Exact matches
%% are preferred over wildcard-option matches.
%%
%% Returns `{ok, Option, Spec, Bindings}` on hit, where `Bindings`
%% is a map from variable name to the matched segment value.
%% Returns `{error, not_found}` or `{error, ambiguous_wildcard_option}` on
%% miss / collision.
-spec resolve(Option) -> Return when
	Option :: list(),
	Return :: {ok, list(), map(), map()}
		| {error, term()}.
resolve(Option) ->
	case spec(Option) of
		{ok, _, Spec} ->
			{ok, Option, Spec, #{}};
		_ ->
			resolve_wildcard_option(Option)
	end.

resolve_wildcard_option(Option) ->
	All = ets:tab2list(?MODULE),
	Matches = lists:filtermap(
		fun({WildcardOption, Spec}) ->
			case match_wildcard_option(WildcardOption, Option, #{}) of
				{ok, Bindings} -> {true, {WildcardOption, Spec, Bindings}};
				nomatch -> false
			end
		end,
		All
	),
	case Matches of
		[] ->
			{error, not_found};
		[{_WildcardOption, Spec, Bindings}] ->
			{ok, Option, Spec, Bindings};
		Many ->
			{error, {ambiguous_wildcard_option, Option,
				[T || {T, _, _} <- Many]}}
	end.

match_wildcard_option([], [], Bindings) ->
	{ok, Bindings};
match_wildcard_option([{VarName} | T1], [Value | T2], Bindings)
		when is_atom(VarName) ->
	match_wildcard_option(T1, T2, Bindings#{ VarName => Value });
match_wildcard_option([H | T1], [H | T2], Bindings) ->
	match_wildcard_option(T1, T2, Bindings);
match_wildcard_option(_, _, _) ->
	nomatch.

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

%% @doc Every live config entry whose key starts with `Prefix'.
%% Covers two sources:
%%   1. Concrete-spec options (e.g. `[debug]', `[semaphores, get_chunk,
%%      limit]'): name lives in the registry as a literal spec; value
%%      comes from the store, or from the spec's `default' if unset.
%%   2. Wildcard-instance entries (e.g. `[peers, <<"1.2.3.4">>,
%%      trusted]'): the registry has only a template spec
%%      (`[peers, {peer_id}, trusted]') — the concrete instance keys
%%      exist solely in the store, written by user config.
%% Callers see a single merged list and don't need to know which
%% namespace shape produced each entry.
-spec get_all_with_prefix(list()) -> [{list(), term()}].
get_all_with_prefix(Prefix) ->
	ConcreteSpecEntries = [
		{Key, V}
		|| {Key, _Spec} <- ets:tab2list(?MODULE),
		   is_list(Key),
		   lists:prefix(Prefix, Key),
		   not is_wildcard(Key),
		   {ok, V} <- [?MODULE:get(Key)]
	],
	StoreEntries = arweave_config_store:items_with_prefix(Prefix),
	ConcreteKeys = sets:from_list([K || {K, _} <- ConcreteSpecEntries]),
	InstanceEntries = [
		E || {K, _} = E <- StoreEntries,
		     not sets:is_element(K, ConcreteKeys)
	],
	ConcreteSpecEntries ++ InstanceEntries.

is_wildcard(Key) ->
	lists:any(fun({_}) -> true; (_) -> false end, Key).

%% @doc Read a value via the registry.
get(Option) ->
	%% Bypass the gen_server when the caller is already running on
	%% the registry process — e.g. a handle_get/handle_set callback
	%% re-entering the public API would otherwise deadlock.
	case whereis(?MODULE) of
		Self when Self =:= self() ->
			get_local(Option);
		_ ->
			gen_server:call(?MODULE, {get, Option}, 10_000)
	end.

%% @doc Set a value via the registry. Validates the key + value, runs
%% any `handle_set` side effect, and stores the value in
%% `arweave_config_store`.
%%
%% == Examples ==
%%
%% ```
%% {ok, NewValue = true, OldValue = false} =
%%   set([global, debug], <<"true">>).
%% '''
-spec set(Option, Value) -> Return when
	Option :: [atom() | iolist()],
	Value :: term(),
	Return :: {ok, term(), term()}
		| {error, term()}.
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
	Return :: {ok, term(), term()} | {error, term()}.
set_local(Option, Value) ->
	do_set(Option, Value).

%% @doc Read an option without going through the gen_server. Safe
%% from inside a `handle_get/2` or `handle_set/4` callback that needs
%% to read another option without deadlocking.
-spec get_local(Option) -> Return when
	Option :: list(),
	Return :: {ok, term()} | {error, term()}.
get_local(Option) ->
	do_get(Option).

-spec init(Specs) -> Return when
	Specs :: [atom() | map()],
	Return :: {ok, NamedEts},
	NamedEts :: ?MODULE.
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

handle_call({get, Option}, _From, State) ->
	case do_get(Option) of
		{ok, Value} ->
			{reply, {ok, Value}, State};
		Else ->
			{reply, Else, State}
	end;
handle_call({set, Option, Value}, _From, State) ->
	{reply, do_set(Option, Value), State};
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
			do_set_runtime(Option, Value, Spec, Bindings);
		Else ->
			Else
	end.

%% Enforce the runtime-writability guard. During load every spec
%% accepts sets; once in runtime mode only `runtime => true` specs do.
do_set_runtime(Option, Value, Spec, Bindings) ->
	RuntimeWritable = maps:get(runtime, Spec, false),
	InRuntime = arweave_config:is_runtime(),
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
	case check(Option, Value, Spec) of
		{ok, Return, _} ->
			do_set_value(Option, Return, Spec, Bindings);
		Else ->
			Else
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
			{ok, OldValue, OldValue};
		{ok, NewValue} ->
			{ok, NewValue, OldValue};
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

%% Run validators after every runtime-mode store; roll back on
%% rejection. During load mode the store is transient and validation
%% is deferred to `arweave_config:runtime/0`.
do_set_store_with_validation(Option, NewValue, OldValue, Spec) ->
	Result = do_set_store(Option, NewValue, OldValue, Spec),
	case {Result, arweave_config:is_runtime()} of
		{{ok, _, _}, true} ->
			case arweave_config_validate:run() of
				ok ->
					Result;
				{error, Reason} ->
					rollback(Option, NewValue, OldValue, Spec),
					{error, Reason}
			end;
		_ ->
			Result
	end.

rollback(Option, _NewValue, undefined, _Spec) ->
	catch arweave_config_store:delete(Option);
rollback(Option, NewValue, OldValue, Spec) ->
	catch do_set_store(Option, OldValue, NewValue, Spec).

do_set_store(Option, NewValue, OldValue, _Spec) ->
	try arweave_config_store:set(Option, NewValue) of
		{ok, {_, _}} ->
			{ok, NewValue, OldValue};
		Else ->
			Else
	catch
		E:R ->
			{error, {E, R}}
	end.

do_get(Option) ->
	case resolve(Option) of
		{ok, Option, Spec, _Bindings} ->
			do_get2(Option, Spec);
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
	Value = arweave_config_store:get(Option, Default),
	{ok, Value};
do_get2(Option, _Spec) ->
	arweave_config_store:get(Option).

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
