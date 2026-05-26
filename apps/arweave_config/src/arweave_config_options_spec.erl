%%% @doc Declarative core of the option spec system: defines what an
%%% option is, lists the contributor modules, and provides the pure
%%% spec-processing utilities used to assemble, normalize, and reflect
%%% on specs.
%%%
%%% The runtime store and CRUD pipeline live in
%%% `arweave_config_options_registry`.
-module(arweave_config_options_spec).
-compile(warnings_as_errors).
-export([
	all/0,
	group_order/0,
	is_function_exported/3,
	normalize_specs/1,
	option_modules/0
]).
-include_lib("kernel/include/logger.hrl").

%% @doc Spec-contributor modules in declared order. Each module
%% contributes a slice of the configuration surface via `specs/0` and
%% optionally a cross-cutting validator via `validate/0`. List order
%% is the canonical group-print order for help output.
-spec option_modules() -> [atom()].
option_modules() ->
	[
		arweave_config_options_misc,
		arweave_config_options_peers,
		arweave_config_options_mining,
		arweave_config_options_storage_modules,
		arweave_config_options_vdf,
		arweave_config_options_transactions,
		arweave_config_options_join,
		arweave_config_options_cm,
		arweave_config_options_sync,
		arweave_config_options_gossip,
		arweave_config_options_packing,
		arweave_config_options_semaphores,
		arweave_config_options_disk_pool,
		arweave_config_options_network,
		arweave_config_options_pool,
		arweave_config_options_randomx,
		arweave_config_options_rocksdb,
		arweave_config_options_repack_modules,
		arweave_config_options_webhooks,
		arweave_config_options_features,
		arweave_config_options_logging,
		arweave_config_options_verify,
		arweave_config_options_defragmentation,
		arweave_config_options_genesis
		% arweave_config_options_config
	].

%% @doc Group atoms in the order their contributor modules appear in
%% `option_modules/0`.
-spec group_order() -> [atom()].
group_order() ->
	[group_of(M) || M <- option_modules()].

%% Fields a spec map may carry, in normalization order. `option_key`
%% must come first so derived metadata (env name, long argument) is
%% available to downstream rules.
field_order() ->
	[
		option_key,
		enabled,
		default,
		handle_get,
		handle_set,
		type,
		legacy,
		short_description,
		long_description,
		nargs,
		runtime,
		deprecated
	].

%% Per-field validation and normalization rules. Each entry declares
%% required/optional/default and either a validate+normalize pair or
%% a custom transform via the `apply` key.
field_rules() ->
	#{
		%% The option name (canonically a list of atoms).
		option_key => #{
			required => true,
			apply => fun apply_option_key/2
		},
		%% Whether the option is active.
		enabled => #{
			default => true,
			validate => fun valid_enabled/1,
			normalize => fun identity/1,
			effect => fun maybe_skip_disabled/2
		},
		%% Default value returned when nothing is stored.
		default => #{
			optional => true,
			validate => fun valid_any/1,
			normalize => fun identity/1,
			on_callback_error => skip
		},
		%% Custom read callback: `fun(OptionKey, State) -> {ok, V} | _`.
		handle_get => #{
			optional => true,
			apply => fun apply_handle_get/2
		},
		%% Custom write callback: `fun(K, V, State, Args) -> ignore | {ok|store, V} | {error, _}`.
		handle_set => #{
			optional => true,
			apply => fun apply_handle_set/2
		},
		%% Value type for parsing/validation (atom or list of atoms).
		type => #{
			optional => true,
			validate => fun valid_type/1,
			normalize => fun normalize_type/1
		},
		%% Legacy field name (atom) bridging this spec to its
		%% historical name.
		legacy => #{
			optional => true,
			validate => fun valid_legacy/1,
			normalize => fun identity/1
		},
		%% One-line user-facing description; falls back to a
		%% humanized form of the option_key when omitted.
		short_description => #{
			optional => true,
			validate => fun valid_optional_iolist/1,
			normalize => fun normalize_short_description/1
		},
		%% Long-form user-facing description for help/docs output.
		long_description => #{
			optional => true,
			validate => fun valid_optional_iolist/1,
			normalize => fun identity/1
		},
		%% Argparse `nargs` spec — number of CLI tokens this option
		%% consumes (integer, `list`, `nonempty_list`, `all`, `maybe`).
		nargs => #{
			optional => true,
			validate => fun valid_nargs/1,
			normalize => fun identity/1
		},
		%% If true, writes are still accepted after the load→runtime
		%% transition. Defaults to false: most options are load-only.
		runtime => #{
			default => false,
			validate => fun is_boolean/1,
			normalize => fun identity/1
		},
		%% Mark the option deprecated. `true` or `{true, Message}` emits
		%% a warning when the option is set.
		deprecated => #{
			default => false,
			validate => fun valid_deprecated/1,
			normalize => fun normalize_deprecated/1
		}
	}.

%%====================================================================
%% Spec assembly
%%====================================================================

%% @doc Assembled list of option spec maps from every contributor.
-spec all() -> [map()].
all() ->
	Specs = lists:flatmap(fun tag_with_group/1, option_modules()),
	with_descriptions(Specs).

%% Tag each contributor spec with its source group and (optional)
%% group description. Group comes from the contributor's `group/0`
%% callback when exported, otherwise from the module name with the
%% `arweave_config_options_` prefix stripped.
tag_with_group(Module) ->
	Group = group_of(Module),
	GroupDesc = group_description_of(Module),
	[
		(case GroupDesc of
			undefined -> Spec#{ group => Group };
			Desc -> Spec#{ group => Group, group_description => Desc }
		end)
		|| Spec <- Module:specs()
	].

group_of(Module) ->
	case erlang:function_exported(Module, group, 0) of
		true ->
			Module:group();
		false ->
			derive_group_from_module(Module)
	end.

group_description_of(Module) ->
	case erlang:function_exported(Module, group_description, 0) of
		true -> Module:group_description();
		false -> undefined
	end.

derive_group_from_module(Module) ->
	Name = atom_to_list(Module),
	Prefix = "arweave_config_options_",
	case lists:prefix(Prefix, Name) of
		true ->
			list_to_atom(lists:nthtail(length(Prefix), Name));
		false ->
			Module
	end.

%% Fill in missing `short_description` / `long_description` from
%% the option_key fallback across every spec.
with_descriptions(Options) ->
	[with_description(Option) || Option <- Options].

%% A missing `long_description` defaults to the short one; a missing
%% `short_description` falls back to a humanized form of the
%% option_key.
with_description(#{ option_key := Key } = Option) ->
	Short = resolve_description(
		maps:get(short_description, Option, undefined),
		fallback_description(Key)
	),
	Long = resolve_description(
		maps:get(long_description, Option, undefined),
		Short
	),
	Option#{
		short_description => Short,
		long_description => Long
	}.

resolve_description(undefined, Fallback) -> Fallback;
resolve_description(<<>>, Fallback) -> Fallback;
resolve_description("", Fallback) -> Fallback;
resolve_description(Value, _Fallback) -> Value.

fallback_description(OptionKey) ->
	Humanized = list_to_binary(
		lists:join(
			<<" ">>,
			[humanize_segment(Item) || Item <- OptionKey]
		)
	),
	<<"Configure ", Humanized/binary, ".">>.

humanize_segment(Item) when is_atom(Item) ->
	binary:replace(atom_to_binary(Item), <<"_">>, <<" ">>, [global]);
humanize_segment(Item) when is_integer(Item) ->
	integer_to_binary(Item);
humanize_segment(Item) when is_binary(Item) ->
	Item;
humanize_segment(Item) when is_list(Item) ->
	unicode:characters_to_binary(Item);
humanize_segment({VarName}) when is_atom(VarName) ->
	<<"<", (atom_to_binary(VarName))/binary, ">">>.

%% @doc Check whether a function is exported by a module.
-spec is_function_exported(Module, Function, Arity) -> Return when
	Module :: atom(),
	Function :: atom(),
	Arity :: pos_integer(),
	Return :: boolean().
is_function_exported(Module, Function, Arity) ->
	try
		Exports = Module:module_info(exports),
		proplists:get_value(Function, Exports, undefined)
	of
		undefined -> false;
		A when A =:= Arity -> true;
		_ -> false
	catch
		_:_ -> false
	end.

%%% --------------------------------------------------------------------
%%% Spec normalization
%%% --------------------------------------------------------------------
-spec normalize_specs([map() | atom()]) -> {ok, map()} | {error, term()}.
normalize_specs(Specs) when is_list(Specs) ->
	normalize_specs_loop(Specs, #{}).

normalize_specs_loop([], Acc) ->
	{ok, Acc};
normalize_specs_loop([Subject | Rest], Acc) ->
	case normalize_subject(Subject) of
		{ok, #{option_key := K} = R} ->
			normalize_specs_loop(Rest, Acc#{K => R});
		discard ->
			normalize_specs_loop(Rest, Acc);
		{discard, _} ->
			normalize_specs_loop(Rest, Acc);
		{error, _} = Error ->
			Error
	end.

-spec normalize_subject(map() | atom()) ->
	{ok, map()} | discard | {discard, term()} | {error, term()}.
normalize_subject(Subject) ->
	case apply_rules(field_order(), Subject, #{}) of
		{ok, State} ->
			{ok, State};
		skip ->
			discard;
		{error, _} = Error ->
			Error
	end.

apply_rules([], _Subject, State) ->
	{ok, State};
apply_rules([Field | Rest], Subject, State) ->
	case apply_rule(Field, maps:get(Field, field_rules()), Subject, State) of
		{ok, State2} ->
			apply_rules(Rest, Subject, State2);
		skip ->
			skip;
		{error, _} = Error ->
			Error
	end.

apply_rule(Field, Rule, Subject, State) ->
	case maps:get(apply, Rule, undefined) of
		undefined ->
			apply_value_rule(Field, Rule, Subject, State);
		Apply ->
			Apply(Subject, State)
	end.

apply_value_rule(Field, Rule, Subject, State) ->
	case raw_field_value(Field, Rule, Subject) of
		missing ->
			{ok, State};
		{error, Reason} ->
			{error, Reason};
		skip ->
			{ok, State};
		{value, Value} ->
			apply_rule_value(Field, Rule, Value, State)
	end.

apply_rule_value(Field, Rule, Value, State) ->
	Validate = maps:get(validate, Rule),
	case Validate(Value) of
		true ->
			Normalize = maps:get(normalize, Rule, fun identity/1),
			Normalized = Normalize(Value),
			case Normalized of
				skip_field ->
					{ok, State};
				_ ->
					State2 = State#{Field => Normalized},
					Effect = maps:get(effect, Rule, fun keep/2),
					Effect(Normalized, State2)
			end;
		false ->
			{error, #{
				field => Field,
				value => Value,
				reason => invalid
			}}
	end.

raw_field_value(Field, Rule, Subject) when is_map(Subject) ->
	case Subject of
		#{Field := Value} ->
			{value, Value};
		_ ->
			raw_default_value(Rule)
	end;
raw_field_value(Field, Rule, Module) when is_atom(Module) ->
	case is_function_exported(Module, Field, 0) of
		true ->
			try
				{value, erlang:apply(Module, Field, [])}
			catch
				E:R:S ->
					handle_field_callback_error(Field, Rule, Module, E, R, S)
			end;
		false ->
			raw_default_value(Rule)
	end.

raw_default_value(#{default := Default}) ->
	{value, Default};
raw_default_value(#{optional := true}) ->
	missing.

handle_field_callback_error(Field, #{on_callback_error := skip}, Module, E, R, S) ->
	?LOG_ERROR([
		{module, ?MODULE},
		{option, Module},
		{field, Field},
		{error, {E, R, S}}
	]),
	skip;
handle_field_callback_error(Field, _Rule, Module, E, R, S) ->
	{error, #{
		module => Module,
		field => Field,
		error => E,
		reason => R,
		stack => S
	}}.

valid_enabled(Value) ->
	is_boolean(Value) orelse is_disabled_with_reason(Value).

valid_deprecated(Value) ->
	is_boolean(Value) orelse is_deprecated_with_message(Value).

valid_any(_Value) ->
	true.

valid_type(Type) when is_atom(Type) ->
	true;
valid_type(Types) when is_list(Types) ->
	lists:all(fun is_atom/1, Types);
valid_type(_Type) ->
	false.

valid_optional_iolist(undefined) ->
	true;
valid_optional_iolist(Value) ->
	is_binary(Value) orelse is_list(Value).

valid_nargs(nonempty_list) ->
	true;
valid_nargs(list) ->
	true;
valid_nargs(all) ->
	true;
valid_nargs('maybe') ->
	true;
valid_nargs({'maybe', _Term}) ->
	true;
valid_nargs(Nargs) when is_integer(Nargs), Nargs >= 0 ->
	true;
valid_nargs(_Nargs) ->
	false.

valid_legacy(Legacy) when is_atom(Legacy) ->
	%% Free-form atom: spec contributors are responsible for picking a
	%% stable name that matches the historical CLI / config.json
	%% field.
	true;
valid_legacy(_Legacy) ->
	false.

is_disabled_with_reason({false, _Reason}) ->
	true;
is_disabled_with_reason(_) ->
	false.

is_deprecated_with_message({true, _Message}) ->
	true;
is_deprecated_with_message(_) ->
	false.

normalize_deprecated({true, _Message}) ->
	true;
normalize_deprecated(Value) ->
	Value.

normalize_short_description(undefined) ->
	skip_field;
normalize_short_description(Value) ->
	Value.

normalize_type(Type) when is_atom(Type) ->
	warn_unknown_type(Type),
	Type;
normalize_type(Types) when is_list(Types) ->
	[begin warn_unknown_type(Type), Type end || Type <- Types].

warn_unknown_type(Type) ->
	case is_function_exported(arweave_config_type, Type, 1) of
		true ->
			ok;
		false ->
			?LOG_WARNING("non existing type ~p", [Type])
	end.

maybe_skip_disabled(false, _State) ->
	skip;
maybe_skip_disabled({false, _Reason}, _State) ->
	skip;
maybe_skip_disabled(_Value, State) ->
	{ok, State}.

keep(_Value, State) ->
	{ok, State}.

identity(Value) ->
	Value.

%%% --------------------------------------------------------------------
%%% option_key
%%% --------------------------------------------------------------------

apply_option_key(Subject, State) ->
	init_option_key(Subject, State).

init_option_key(Map = #{option_key := CK}, State) when is_map(Map) ->
	check_option_key(Map, CK, State);
init_option_key(Map, State) when is_map(Map) ->
	{error, #{
		module => Map,
		reason => missing_parameter_key,
		state => State
	}};
init_option_key(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, option_key, 0) of
		true ->
			try
				CK = Module:option_key(),
				check_option_key(Module, CK, State)
			catch
				_:Reason ->
					{error, Reason}
			end;
		false ->
			{error, #{
				callback => option_key,
				reason => parameter_key_not_defined,
				module => Module,
				state => State
			}}
	end.

check_option_key(Module, CK, State) when is_binary(CK) ->
	case arweave_config_parser:key(CK) of
		{ok, Value} ->
			{ok, derive_option_key_metadata(Value, State#{option_key => Value})};
		{error, Reason} ->
			{error, #{
				module => Module,
				callback => option_key,
				reason => {Reason, CK},
				state => State
			}}
	end;
check_option_key(Module, CK, State) when is_list(CK) ->
	check_option_key_segments(Module, CK, CK, State);
check_option_key(Module, CK, State) ->
	{error, #{
		callback => option_key,
		reason => {invalid, CK},
		module => Module,
		state => State
	}}.

check_option_key_segments(Module, [], [], State) ->
	{error, #{
		reason => {invalid, []},
		module => Module,
		state => State,
		callback => option_key
	}};
check_option_key_segments(_Module, [], CK, State) ->
	{ok, derive_option_key_metadata(CK, State#{option_key => CK})};
check_option_key_segments(Module, [Item | Rest], CK, State) when is_atom(Item) ->
	check_option_key_segments(Module, Rest, CK, State);
check_option_key_segments(Module, [Item | Rest], CK, State) when is_binary(Item) ->
	check_option_key_segments(Module, Rest, CK, State);
check_option_key_segments(Module, [{Variable} | Rest], CK, State) when is_atom(Variable) ->
	check_option_key_segments(Module, Rest, CK, State);
check_option_key_segments(Module, [Item | Rest], _CK, State) ->
	{error, #{
		callback => option_key,
		reason => {invalid, Item},
		module => Module,
		state => State,
		rest => Rest
	}}.

derive_option_key_metadata(OptionKey, State) ->
	State#{
		environment => environment_convert(OptionKey),
		long_argument => long_argument_convert(OptionKey)
	}.

%%% --------------------------------------------------------------------
%%% handle_get
%%% --------------------------------------------------------------------

apply_handle_get(#{handle_get := Get} = _Subject, State) when is_function(Get, 2) ->
	{ok, State#{get => Get}};
apply_handle_get(Subject, State) when is_map(Subject) ->
	{ok, State};
apply_handle_get(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_get, 2) of
		true ->
			{ok, State#{get => fun Module:handle_get/2}};
		false ->
			{ok, State}
	end.

%%% --------------------------------------------------------------------
%%% handle_set
%%% --------------------------------------------------------------------

apply_handle_set(#{handle_set := {Set, Args}} = _Subject, State) when is_function(Set, 4) ->
	{ok, State#{set => Set, set_args => Args}};
apply_handle_set(#{handle_set := Set} = _Subject, State) when is_function(Set, 4) ->
	{ok, State#{set => Set, set_args => []}};
apply_handle_set(Subject, State) when is_map(Subject) ->
	{ok, State};
apply_handle_set(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_set, 4) of
		true ->
			State1 = State#{set => fun Module:handle_set/4},
			case is_function_exported(Module, set_args, 0) of
				true ->
					Args = Module:set_args(),
					{ok, State1#{set_args => Args}};
				false ->
					{ok, State1}
			end;
		false ->
			{ok, State}
	end.

environment_convert(PK) ->
	environment_convert(PK, []).

environment_convert([], Buffer) ->
	Reverse = lists:reverse(Buffer),
	Join = lists:join(<<"_">>, [<<"AR">> | Reverse]),
	list_to_binary(Join);
environment_convert([H | T], Buffer) when is_atom(H) ->
	Bin = atom_to_binary(H),
	Upper = string:uppercase(Bin),
	environment_convert(T, [Upper | Buffer]);
environment_convert([H | T], Buffer) when is_integer(H) ->
	Bin = integer_to_binary(H),
	environment_convert(T, [Bin | Buffer]);
environment_convert([H | T], Buffer) when is_binary(H) ->
	environment_convert(T, [H | Buffer]);
environment_convert([{Var} | T], Buffer) when is_atom(Var) ->
	Bin = atom_to_binary(Var),
	Upper = string:uppercase(Bin),
	environment_convert(T, [Upper | Buffer]).

long_argument_convert(List) when is_list(List) ->
	long_argument_convert(List, []);
long_argument_convert(<<"-", _/binary>> = Binary) ->
	Binary;
long_argument_convert(Binary) when is_binary(Binary) ->
	<<"-", Binary/binary>>.

long_argument_convert([], Buffer) ->
	Sep = application:get_env(arweave_config, long_argument_separator, "."),
	Bin = list_to_binary(lists:join(Sep, lists:reverse(Buffer))),
	<<"--", Bin/binary>>;
long_argument_convert([H | T], Buffer) when is_integer(H) ->
	long_argument_convert([integer_to_binary(H) | T], Buffer);
long_argument_convert([H | T], Buffer) when is_atom(H) ->
	long_argument_convert([atom_to_binary(H) | T], Buffer);
long_argument_convert([H | T], Buffer) when is_list(H) ->
	long_argument_convert([list_to_binary(H) | T], Buffer);
long_argument_convert([H | T], Buffer) when is_binary(H) ->
	long_argument_convert(T, [H | Buffer]);
long_argument_convert([{VarName} | T], Buffer) when is_atom(VarName) ->
	Rendered = <<"[", (atom_to_binary(VarName))/binary, "]">>,
	long_argument_convert(T, [Rendered | Buffer]).

