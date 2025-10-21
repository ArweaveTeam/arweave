%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc Arweave configuration specification behavior.
%%%
%%% When used  as module, `arweave_config_spec' defines  a behavior to
%%% deal with arweave parameters.
%%%
%%% When used as process, `arweave_config_spec' is in charge of
%%% loading and  managing arweave configuration  specification, stored
%%% in a map.
%%%
%%% == `arweave_config_spec' process ==
%%%
%%% The    `arweave_config_spec'   process    is   a    frontend   for
%%% `arweave_config_store'.  The idea  is to  pass the  parameter from
%%% another module/process, check it first based on the specification,
%%% and then  forward the valid  result to  store it. here  an example
%%% anwith the environment
%%%
%%% ```
%%%  _____________
%%% |             |
%%% | system      |
%%% | environment |
%%% |_____________|
%%%    |
%%%    |
%%%  _\_/________________________
%%% |                            |
%%% | arweave_config_environment |<--+
%%% |____________________________|   |
%%%    |      / \                    |
%%%    |       | [specification      |
%%%    |       |  errors]            |
%%%  _\ /______|__________           |
%%% |                     |          |
%%% | arweave_config_spec |          |
%%% |_____________________|          |
%%%    |                             |
%%%    | [specification success]     |
%%%    |                             |
%%%  _\_/__________________          |
%%% |                      |         |
%%% | arweave_config_store |---------+ [valid result]
%%% |______________________|
%%%
%%% '''
%%%
%%% == TODO ===
%%%
%%% === Check Parameter Function ===
%%%
%%% A function is required to check manually/on demande a value
%%% without setting it. It will be needed for testing.
%%%
%%% === Variable Parameter Item Specification ===
%%%
%%% Parameter item can be a variable, defined by a `type'. This is
%%% helpful when setting different kind of values like storage modules
%%% or peers.
%%%
%%% ```
%%% [peers, {peer}, enabled]
%%% '''
%%%
%%% === Check for duplicated values ===
%%%
%%% A warning (or an error) should be returned when there is a
%%% duplicated specification. Here a quick list of error/warning:
%%%
%%% - errors (stop execution):
%%%   - duplicated parameter_key
%%% - warnings (last one overwrite the first one):
%%%   - duplicated environments
%%%   - duplicated short arguments
%%%   - duplicated long arguments
%%%   - duplicated legacy
%%%
%%% ===  Improve callback functions ===
%%%
%%% Add support for pre/post actions.
%%%
%%% Add support for MFA
%%%
%%% Add support for embedded lambdas
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec).
-behavior(gen_server).
-export([
	start_link/0,
	start_link/1,
	stop/0,
	spec/0,
	spec/1,
	spec_to_argparse/0,
	get_default/1,
	get_legacy/0,
	get_environments/0,
	get_environment/1,
	get_short_arguments/0,
	get_short_argument/1,
	get_long_arguments/0,
	get_long_argument/1,
	get/1,
	set/2
]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([is_function_exported/3]).
-compile({no_auto_import,[get/1]}).
-include_lib("kernel/include/logger.hrl").

% A raw key from external sources (cli, api...), non-sanitized,
% non-parsed.
-type key() :: term().

% An arweave parameter, parsed and valid, containing only known
% terms and specified.
-type parameter() :: [atom() | {atom()} | binary()].

% A value associated with a key/parameter, usually any kind of term.
-type value() :: term().

%---------------------------------------------------------------------
% REQUIRED:  defines the  configuration key  used to  identify arweave
% parameter, usually stored in a data store like ETS.
%---------------------------------------------------------------------
-callback parameter_key() -> Return when
	Return :: parameter().

%---------------------------------------------------------------------
% OPTIONAL: defines how to retrieve the value using the parameter key.
%---------------------------------------------------------------------
-callback handle_get(ParameterKey, State) -> Return when
	ParameterKey :: parameter(),
	State :: map(),
	Return :: {ok, value()}
		| {ok, Action}
		| {ok, MFA}
		| {error, map()},
	CallbackReturn :: {ok, term()} | {error, term()},
	Action :: fun ((ParameterKey, State) -> CallbackReturn),
	MFA :: {atom(), atom(), list()}.

%---------------------------------------------------------------------
% OPTIONAL: defines how to set the value Value with the parameter key.
% It should be transaction. This callback must be improved, instead
% of returning directly a value, it should also be possible to return
% a list of MFA or lambda functions executed in order like
% transactions.
%
% `ignore' will keep the old value in place.
%
% `{ok, term()}' will simply returns the term, the side effect is
% protected during the execution, and the module can do whatever (even
% setting the value anywhere).
%
% `{store, term()}' will automatically store the returned value into
% `arweave_config_store' using `arweave_config_store:set/2' function.
%
% `{error, map()}' will return an error and the reason.
%
% == TODO ==
%
% `{ok, action() | actions()}' will execute a list of action in order
% first from last. Those are function with side effects, and their
% return is not controlled. Those action should be defined as `fun/3':
%
% ```
% Action = fun (Parameter, NewValue, OldValue) ->
%   % do some action...
% end.
% '''
%
% `{ok, mfa() | mfas()}' same than the previous action definition.
%
% ```
% -module(my_module).
% export([mfa/3]).
% mfa(Parameter, NewValue, OldValue) ->
%   ok.
% '''
%
%---------------------------------------------------------------------
-callback handle_set(Parameter, NewValue, State) -> Return when
	Parameter:: parameter(),
	NewValue :: value(),
	State :: map(),
	% CallbackReturn :: {ok, term()} | {error, term()},
	% Action :: fun ((Parameter, Value) -> CallbackReturn),
	% Actions :: [Action],
	% MFA :: {atom(), atom(), list()},
	% MFAs :: [MFA],
	Return :: ignore
		| {ok, term()}
		| {store, term()}
		% | {ok, Action}
		% | {ok, Actions}
		% | {ok, MFA}
		% | {ok, MFAs}
		| {error, map()}.

%---------------------------------------------------------------------
% OPTIONAL: defines  if the  parameter can be  set during  runtime. if
% true, the  parameter can be set  when arweave is running,  else, the
% parameter can only be set during startup
% DEFAULT: false
%---------------------------------------------------------------------
-callback runtime() -> Return when
	Return :: boolean().

%---------------------------------------------------------------------
% OPTIONAL: short argument used to  configure the parameter, usually a
% single ASCII letter present in range 0-9, a-z and A-Z.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback short_argument() -> Return when
	Return :: undefined
		| [pos_integer()].

%---------------------------------------------------------------------
% OPTIONAL: a long argument, used  to configure the parameter, usually
% lower cases words separated by dashes.
% DEFAULT: converted parameter key (e.g. --global.debug)
%---------------------------------------------------------------------
-callback long_argument() -> Return when
	Return :: undefined
		| iolist().

%---------------------------------------------------------------------
% OPTIONAL: the type of the value.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback type() -> Return when
	Return :: undefined
		| atom()
		| fun ((term()) -> {ok, term()} | {error, term()}).

%---------------------------------------------------------------------
% OPTIONAL: a function returning  a string representing an environment
% variable.
% DEFAULT: false
%---------------------------------------------------------------------
-callback environment() -> Return when
	Return :: undefined
		| string().

%---------------------------------------------------------------------
% OPTIONAL: a list  of legacy references used to  previously fetch the
% value.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback legacy() -> Return when
	Return :: undefined
		| atom()
		| iolist()
		| [atom() | iolist()].

%---------------------------------------------------------------------
% OPTIONAL: a short description of the parameter.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback short_description() -> Return when
	Return :: undefined
		| iolist().

%---------------------------------------------------------------------
% OPTIONAL: a long description of the parameter.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback long_description() -> Return when
	Return :: undefined
		| iolist().

%---------------------------------------------------------------------
% OPTIONAL: defines if a parameter is deprecated, can eventually
% returns a message.
% DEFAULT: false
%---------------------------------------------------------------------
-callback deprecated() -> Return when
	Return :: true
		| {true, term()}
		| false.

%--------------------------------------------------------------------
% OPTIONAL: defines the number of arguments to take
% DEFAULT: 1
% see: argparse
%--------------------------------------------------------------------
-callback nargs() -> Return when
	Return :: pos_integer()
		| list
		| nonempty_list
		| 'maybe'
		| {'maybe', term()}
		| all.

%---------------------------------------------------------------------
% @TODO: protected callback
% OPTIONAL: defines if the value should be public or protected (not
% displayed or even encrypted, useful for password)
% DEFAULT: false
%---------------------------------------------------------------------
% -callback protected() -> Return when
%       Return :: boolean().

%---------------------------------------------------------------------
% @TODO: dependencies callback
% OPTIONAL: defines a list of required parameters to be set
% DEFAULT: undefined
%---------------------------------------------------------------------
% -callback dependencies() -> Return when
%	Return :: undefined | [atom()|iolist()|tuple()].

%---------------------------------------------------------------------
% @TODO: conflicts callback
% OPTIONAL: defines a list of conflicting parameters
% DEFAULT: undefined
%---------------------------------------------------------------------
% -callback conflicts() -> RETURN when
%	Return :: undefined | [atom()|iolist()|tuple()].

%---------------------------------------------------------------------
% @TODO: formatter callback
% OPTIONAL: defines a function callback to format short or long
% help message.
% DEFAULT: undefined
%---------------------------------------------------------------------
% -callback formatter(Type, Value) when
%	Type :: short | long,
%	Value :: iolist(),
%	Return :: undefined | {ok, FormattedValue},
%	FormattedValue :: iolist().

%---------------------------------------------------------------------
% @TODO: positional arguments callback
% OPTIONAL: defines if the argument is positional, those are found
% after a special separator, usually `--'.
%---------------------------------------------------------------------
% -callback positional() -> Return when
%	Return :: boolean().

%---------------------------------------------------------------------
% @TODO: before/after arguments callback
% OPTIONAL: defines if another argument should be set before or after
% the current one
%---------------------------------------------------------------------
% -callback before() -> Return when
%	Return :: undefined | [atom()].
% -callback after() -> Return when
%	Return :: undefined | [atom()].

%---------------------------------------------------------------------
% @TODO: dryrun argument
% OPTIONAL: instead of executing the set callback, it simply returns
% the action and modification would have been applied.
%---------------------------------------------------------------------
% -callback dryrun() -> Return when
%	Return :: term().

%---------------------------------------------------------------------
% @TODO fail callback
% OPTIONAL: defines if a wrong value should stop the execution, with a
% specific error set.
%---------------------------------------------------------------------
% -spec fail() -> Return when
%	Return :: boolean()
%		| {true, term()}.

-optional_callbacks([
	handle_get/2,
	handle_set/3,
	runtime/0,
	short_argument/0,
	long_argument/0,
	type/0,
	environment/0,
	legacy/0,
	short_description/0,
	long_description/0,
	deprecated/0,
	nargs/0
]).

%%--------------------------------------------------------------------
%% @doc Start `arweave_config_spec' process.
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Return when
	Return :: {ok, pid()}.

start_link() ->
	start_link([]).

%%--------------------------------------------------------------------
%% @doc Start `arweave_config_spec' process.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Specs) -> Return when
	Specs :: [map() | atom()],
	Return :: {ok, pid()}.

start_link(Specs) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Specs, []).

%%--------------------------------------------------------------------
%% @doc Stop `arweave_config_spec' process.
%% @end
%%--------------------------------------------------------------------
stop() ->
	gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% @doc returns the full list of specifications.
%% @end
%%--------------------------------------------------------------------
spec() ->
	Pattern = {'$1', '$2'},
	Guard = [],
	Select = [{{'$1', '$2'}}],
	Result = ets:select(?MODULE, [{Pattern, Guard, Select}]),
	maps:from_list(Result).

%%--------------------------------------------------------------------
%% @doc returns parameter's specification from specification store.
%% @end
%%--------------------------------------------------------------------
spec(ParameterSpec) ->
	Pattern = {'$1', '$2'},
	Guard = [{'=:=', '$1', ParameterSpec}],
	Select = [{{'$1', '$2'}}],
	case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
		[{Parameter, Spec}] ->
			{ok, Parameter, Spec};
		_Elsewise ->
			{error, not_found}
	end.

%%--------------------------------------------------------------------
%% @doc returns default parameter value if defined.
%% @end
%%--------------------------------------------------------------------
get_default(Parameter) ->
	Pattern = {'$1', #{ default => '$2'}},
	Guard = [{'=:=', '$1', Parameter}],
	Select = ['$2'],
	case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
		[Default] ->
			{ok, Default};
		_Elsewise ->
			{error, not_found}
	end.

%%--------------------------------------------------------------------
%% @doc returns the list of environment variable supported.
%% @end
%%--------------------------------------------------------------------
get_environments() ->
	Pattern = {'$1', #{ environment => '$2'}},
	Guard = [],
	Select = [{{'$2', '$1'}}],
	ets:select(?MODULE, [{Pattern, Guard, Select}]).

%%--------------------------------------------------------------------
%% @doc Returns the specification for an environment variable.
%% @end
%%--------------------------------------------------------------------
get_environment(EnvironmentKey) ->
	Pattern = {'$1', #{ environment => '$2'}},
	Guard = [{'=:=', '$2', EnvironmentKey}],
	Select = ['$1'],
	case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
		[Parameter] ->
			[{Parameter, Value}] = ets:lookup(?MODULE, Parameter),
			{ok, Parameter, Value};
		_Elsewise ->
			{error, not_found}
	end.

%%--------------------------------------------------------------------
%% @doc Returns the list of short arguments supported with the number
%% of elements required.
%% @end
%%--------------------------------------------------------------------
get_short_arguments() ->
	Pattern = {'$1', #{ short_argument => '$2' }},
	Guard = [],
	Select = [{{'$2', '$_'}}],
	% match spec does not support correctly map, so, a filter
	% is required to cleanup things.
	[
		{Argument, Spec}
		|| {Argument, {_, Spec}}
		<- ets:select(?MODULE, [{Pattern, Guard, Select}])
	].

%%--------------------------------------------------------------------
%% @doc returns specification for a short argument.
%% @end
%%--------------------------------------------------------------------
get_short_argument(ArgumentKey) ->
	Pattern = {'$1', #{ short_argument => '$2' }},
	Guard = [{'=:=', '$2', ArgumentKey}],
	Select = [{{'$2', '$_'}}],
	% match spec does not support correctly map, so, a filter
	% is required to cleanup things.
	[
		{Argument, Spec}
		|| {Argument, {_, Spec}}
		<- ets:select(?MODULE, [{Pattern, Guard, Select}])
	].

%%--------------------------------------------------------------------
%% @doc Returns the list of long arguments supported with the number
%% of elements required.
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @doc Returns specification from a long argument.
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @doc Returns legacy keys (used for legacy configuration
%% compatibility).
%% @end
%%--------------------------------------------------------------------
get_legacy() ->
	Pattern = {'$1', #{ legacy => '$2' }},
	Guard = [{'=/=', '$2', undefined}],
	Select = [{{'$2', '$1'}}],
	Query = [{Pattern, Guard, Select}],
	maps:from_list(ets:select(?MODULE, Query)).

%%--------------------------------------------------------------------
%% @doc get a value using a parameter.
%% @end
%%--------------------------------------------------------------------
get(Parameter) ->
	gen_server:call(?MODULE, {get, Parameter}, 10_000).

%%--------------------------------------------------------------------
%% @doc set a parameters. The process will be in charge to check both
%% keys and values then if everything is good, it will execute a side
%% effect (to modify the application state) and finally store/update
%% the value in `arweave_config_store'.
%%
%% == Examples ==
%%
%% ```
%% {ok, NewValue = true, OldValue = false} =
%%   set([global, debug], <<"true">>).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec set(Parameter, Value) -> Return when
	Parameter :: [atom() | iolist()],
	Value :: term(),
	Return :: {ok, term()}
		| {error, term()}.

set(Parameter, Value) ->
	gen_server:call(?MODULE, {set, Parameter, Value}, 10_000).

%%--------------------------------------------------------------------
%% @hidden
%% @doc Returns a list of module callbacks to check specifications.
%% This function has been created to avoid having to deal with a very
%% long and complex file. Each module callbacks only export an init/2
%% function to initialize the final state corresponding to a spec
%% callback.
%% @end
%%--------------------------------------------------------------------
callbacks_check() -> [
	% mandatory callbacks
	{parameter_key, arweave_config_spec_parameter_key},

	% optional callbacks
	{handle_get, arweave_config_spec_handle_get},
	{handle_set, arweave_config_spec_handle_set},
	{default, arweave_config_spec_default},
	{type, arweave_config_spec_type},
	{runtime, arweave_config_spec_runtime},
	{deprecated, arweave_config_spec_deprecated},
	{environment, arweave_config_spec_environment},
	{short_argument, arweave_config_spec_short_argument},
	{long_argument, arweave_config_spec_long_argument},
	{legacy, arweave_config_spec_legacy},
	{short_description, arweave_config_spec_short_description},
	{long_description, arweave_config_spec_long_description},
	{nargs, arweave_config_spec_nargs}
].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init([]) ->
	Specs = arweave_config_parameters:init(),
	init2(Specs);
init(Specs) when is_list(Specs) ->
	init2(Specs).

init2(Specs) ->
	% catch exception
	erlang:process_flag(trap_exit, true),

	% create the ETS table, this one should be only reachable by
	% the current process to avoid doing nasty things with the
	% specification during runtime.
	Ets = ets:new(?MODULE, [
		named_table,
		protected
	]),

	% parse and load all specifications from each modules.
	{ok, MapSpec} = init_loop(Specs, #{}),

	% insert all specification into the specification store (ETS).
	% @TODO specification must be unique, when inserting a new
	% specification, if the same key exists, a warning should be
	% displayed in some way.
	[
		ets:insert(?MODULE, {K, V})
		||
		{K, V} <- maps:to_list(MapSpec)
	],

	% It should be good, we can start the module.
	?LOG_INFO("~p ready", [?MODULE]),
	{ok, Ets}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_loop([], Buffer) ->
	% no more arguments to path, the buffer is returned, it should
	% contain the complete list of all arguments specification
	% supported.
	{ok, Buffer};
init_loop([Map|Rest], Buffer) when is_map(Map) ->
	% the argument is defined as map, then all callback are
	% checked as map key.
	case init_map(Map, #{}) of
		{ok, #{ parameter_key := K } = R} ->
			init_loop(Rest, Buffer#{ K => R });
		discard ->
			init_loop(Rest, Buffer);
		{discard, _Message} ->
			init_loop(Rest, Buffer);
		Elsewise ->
			Elsewise
	end;
init_loop([Module|Rest], Buffer) when is_atom(Module) ->
	% the argument is defined as module callback, then
	% all callback are checked as functions exported.
	case init_module(Module, #{}) of
		{ok, #{ parameter_key := K } = R} ->
			init_loop(Rest, Buffer#{ K => R });
		discard ->
			init_loop(Rest, Buffer);
		{discard, _Message} ->
			init_loop(Rest, Buffer);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module(Module, State) ->
	CallbacksCheck = callbacks_check(),
	init_module(Module, CallbacksCheck, State).

init_module(Module, [], State) ->
	?LOG_INFO("loaded callback module ~p", [Module]),
	{ok, State};
init_module(Module, [{_Callback, ModuleCallback}|Rest], State) ->
	case erlang:apply(ModuleCallback, init, [Module, State]) of
		{ok, NewState} ->
			?LOG_INFO("checked callback from module ~p:~p", [Module,ModuleCallback]),
			init_module(Module, Rest, NewState);
		Elsewise ->
			?LOG_WARNING("can't load parameter from module ~p:~p", [Module, Elsewise]),
			{discard, Elsewise}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_map(Map, State) ->
	CallbacksCheck = callbacks_check(),
	init_map(Map, CallbacksCheck, State).

init_map(Map, [], State) ->
	?LOG_INFO("loaded callback map ~p", [Map]),
	{ok, State};
init_map(Map, [{_Callback, ModuleCallback}|Rest], State) ->
	case erlang:apply(ModuleCallback, init, [Map, State]) of
		{ok, NewState} ->
			?LOG_INFO("checked callback from map ~p:~p", [Map, ModuleCallback]),
			init_map(Map, Rest, NewState);
		Elsewise ->
			?LOG_WARNING("can't load parameter from map ~p:~p", [Map, Elsewise]),
			{discard, Elsewise}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call({get, Parameter}, _From, State) ->
	case apply_get(Parameter) of
		{ok, Value} ->
			{reply, {ok, Value}, State};
		Elsewise ->
			{reply, Elsewise, State}
	end;
handle_call({set, Parameter, Value}, _From, State) ->
	case apply_set(Parameter, Value) of
		Return = {ok, Return} ->
			{reply, Return, State};
		Elsewise ->
			{reply, Elsewise, State}
	end;
handle_call(Msg, From, State) ->
	?LOG_WARNING([
		{message, Msg},
		{from, From},
		{module, ?MODULE},
		{function, handle_call}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	?LOG_WARNING([
		{message, Msg},
		{module, ?MODULE},
		{function, handle_cast}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_WARNING([
		{message, Msg},
		{module, ?MODULE},
		{function, handle_info}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc Check if a function from a module is exported.
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @hidden
%% @doc A pipeline function to check if the value is correct or not.
%% @end
%%--------------------------------------------------------------------
check(Parameter, Value, Spec) ->
	check_type(Parameter, Value, Spec, #{}).

%%--------------------------------------------------------------------
%% @hidden
%% @doc Check the type of the value associated with the parameter.
%% This function will check for a type present in
%% `arweave_config_type' module and execute it. It should return,
%% `ok', `{ok, ConvertedValue}' or `{error, Term}'.
%% @end
%%--------------------------------------------------------------------
check_type(Parameter, Value, Spec = #{ type := Type }, Buffer) ->
	try
		arweave_config_type:Type(Value)
	of
		ok ->
			NewBuffer = Buffer#{ type => ok },
			check_final(Parameter, Value, Spec, NewBuffer);
		{ok, V} ->
			NewBuffer = Buffer#{ type => ok },
			check_final(Parameter, V, Spec, NewBuffer);
		Error ->
			NewBuffer = Buffer#{ type => Error },
			check_final(Parameter, Value, Spec, NewBuffer)
	catch
		E:R ->
			NewBuffer = Buffer#{ type => {E, R} },
			check_final(Parameter, Value, Spec, NewBuffer)
	end;
check_type(Parameter, Value, Spec, Buffer) ->
	check_final(Parameter, Value, Spec, Buffer#{ type => undefined }).

%%--------------------------------------------------------------------
%% @hidden
%% @doc final check function, all returned values from the previous
%% call should be present in `Buffer' variable.
%% @end
%%--------------------------------------------------------------------
check_final(_, Value, _, Buffer) ->
	?LOG_DEBUG("~p", [Buffer]),
	case Buffer of
		#{ type := undefined } ->
			{ok, Value, Buffer};
		#{ type := ok } ->
			{ok, Value, Buffer};
		_ ->
			{error, Value, Buffer}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% 1. get he specification, if they are present, we can continue
%% to execute the transaction
%%--------------------------------------------------------------------
apply_set(Parameter, Value) ->
	case spec(Parameter) of
		{ok, Parameter, Spec} ->
			apply_set_runtime(Parameter, Value, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @hidden
%% check if the parameter can be set during runtime or not.
%%--------------------------------------------------------------------
apply_set_runtime(Parameter, Value, Spec) ->
	RuntimeMode = maps:get(runtime, Spec, false),
	Runtime = arweave_config:is_runtime(),
	case {Runtime, RuntimeMode} of
		{false, false} ->
			apply_set_parameter(Parameter, Value, Spec);
		{false, true} ->
			apply_set_parameter(Parameter, Value, Spec);
		{true, true} ->
			apply_set_parameter(Parameter, Value, Spec);
		{true, false} ->
			{error, #{
					parameter => Parameter,
					reason => not_a_runtime_parameter,
					value => Value
				}
			}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% 2. check if the specification match the parameter/value,
%% if everything is fine, we can continue the execution
%%--------------------------------------------------------------------
apply_set_parameter(Parameter, Value, Spec) ->
	case check(Parameter, Value, Spec) of
		{ok, Return, _} ->
			apply_set_value(Parameter, Return, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @hidden
%% 3. let retrieve the value (if set) and use the handle_set/3
%% callback to set the value.
%%--------------------------------------------------------------------
apply_set_value(Parameter, Value, Spec = #{ set := Set }) ->
	% if handle_set/3 is present, we execute it.
	Default = maps:get(default, Spec, undefined),
	OldValue = arweave_config_store:get(Parameter, Default),
	State = local_state(#{
		spec => Spec,
		old_value => OldValue
	}),
	try Set(Parameter, Value, State) of
		ignore ->
			{ok, OldValue, OldValue};
		{ok, NewValue} ->
			{ok, NewValue, OldValue};
		{store, NewValue} ->
			apply_set_store(Parameter,NewValue,OldValue,Spec);
		Elsewise ->
			Elsewise
	catch
		E:R ->
			{E,R}
	end;
apply_set_value(Parameter, Value, Spec) ->
	% if no handle_set/3 has been configured, we only store the
	% value by default.
	Default = maps:get(default, Spec, undefined),
	OldValue = arweave_config_store:get(Parameter, Default),
	apply_set_store(Parameter, Value, OldValue, Spec).

%%--------------------------------------------------------------------
%% @hidden
%% 4. the previous callback returned `store', then we store
%% the value into arweave_config_store.
%%--------------------------------------------------------------------
apply_set_store(Parameter, NewValue, OldValue, _Spec) ->
	try arweave_config_store:set(Parameter, NewValue) of
		{ok, {_, _}} ->
			{ok, NewValue, OldValue};
		Elsewise ->
			Elsewise
	catch
		E:R ->
			{E, R}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc
%% @end
%%--------------------------------------------------------------------
apply_get(Parameter) ->
	case spec(Parameter) of
		{ok, Parameter, Spec} ->
			apply_get2(Parameter, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
apply_get2(Parameter, _Spec = #{ get := Get, default := Default }) ->
	State = local_state(#{}),
	case Get(Parameter, #{}) of
		{ok, Value} ->
			{ok, Value};
		_ ->
			{ok, Default}
	end;
apply_get2(Parameter, _Spec = #{ get := Get }) ->
	case Get(Parameter, #{}) of
		{ok, Value} ->
			{ok, Value};
		Elsewise ->
			{error, Elsewise}
	end;
apply_get2(Parameter, _Spec = #{ default := Default }) ->
	Value = arweave_config_store:get(Parameter, Default),
	{ok, Value};
apply_get2(Parameter, _Spec) ->
	arweave_config_store:get(Parameter).

%%--------------------------------------------------------------------
%% @doc Converts arweave configuration specification to argparse map.
%% @see argparse
%% @end
%%--------------------------------------------------------------------
spec_to_argparse() ->
	% fetch the specifications and convert them to proplists, it
	% will be easier to loop over.
	Specs = [
		maps:to_list(X)
		||
		{_, X} <- maps:to_list(spec())
	],
	#{ arguments => spec_to_argparse(Specs, []) }.

% @hidden
spec_to_argparse([], Buffer) -> Buffer;
spec_to_argparse([Spec|Rest], Buffer) ->
	ArgParse = spec_to_argparse2(Spec, #{}),
	spec_to_argparse(Rest, [ArgParse|Buffer]).

% @hidden
spec_to_argparse2([], ArgParse) -> ArgParse;
spec_to_argparse2([{parameter_key, Name}|Rest], ArgParse) ->
	% convert the configuration key parameter to name
	spec_to_argparse2(Rest, ArgParse#{ name => Name });
spec_to_argparse2([{default, Default}|Rest], ArgParse) ->
	spec_to_argparse2(Rest, ArgParse#{ default => Default });
spec_to_argparse2([{nargs, Nargs}|Rest], ArgParse) ->
	spec_to_argparse2(Rest, ArgParse#{ nargs => Nargs });
spec_to_argparse2([{long_argument, Long}|Rest], ArgParse)
	when is_binary(Long) ->
		% arweave config spec uses binary, convert it.
		spec_to_argparse2(Rest, ArgParse#{ long => binary_to_list(Long) });
spec_to_argparse2([{short_argument, Short}|Rest], ArgParse) ->
	spec_to_argparse2(Rest, ArgParse#{ short => Short });
spec_to_argparse2([{required, Required}|Rest], ArgParse) ->
	spec_to_argparse2(Rest, ArgParse#{ required => Required });
spec_to_argparse2([{short_description, SD}|Rest], ArgParse) ->
	spec_to_argparse2(Rest, ArgParse#{ help => SD });
spec_to_argparse2([{type, Type}|Rest], ArgParse) ->
	% At this time, only support boolean type
	case Type of
		boolean ->
			spec_to_argparse2(Rest, ArgParse#{ type => Type });
		_Elsewise ->
			spec_to_argparse2(Rest, ArgParse)
	end;
spec_to_argparse2([Ignore|Rest], ArgParse) ->
	?LOG_DEBUG("ignored value: ~p", [Ignore]),
	spec_to_argparse2(Rest, ArgParse).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
local_state(Map) ->
	maps:merge(Map, #{config => arweave_config_store:to_map()}).
