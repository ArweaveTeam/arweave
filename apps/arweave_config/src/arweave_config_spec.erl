%%%===================================================================
%%% @doc
%%%
%%% Arweave configuration specification behavior.
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
%%% @end
%%%===================================================================
-module(arweave_config_spec).
-behavior(gen_server).
-export([
	 start_link/1,
	 spec/1,
	 get_environment/1,
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
-callback configuration_key() -> Return when
	Return :: parameter().

%---------------------------------------------------------------------
% REQUIRED: defines how to retrieve the value using the parameter key.
%---------------------------------------------------------------------
-callback handle_get(Parameter) -> Return when
	Parameter :: parameter(),
	Return :: {ok, value()}
		| {ok, Action}
		| {ok, MFA}
		| {error, map()},
	CallbackReturn :: {ok, term()} | {error, term()},
	Action :: fun ((Parameter) -> CallbackReturn),
	MFA :: {atom(), atom(), list()}.

%---------------------------------------------------------------------
% REQUIRED: defines how to set the value Value with the parameter key.
% It should be transaction. This callback must be improved, instead
% of returning directly a value, it should also be possible to return
% a list of MFA or lambda functions executed in order like
% transactions.
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
-callback handle_set(Parameter, NewValue, OldValue) -> Return when
	Parameter:: parameter(),
	NewValue :: value(),
	OldValue :: value(),
	% CallbackReturn :: {ok, term()} | {error, term()},
	% Action :: fun ((Parameter, Value) -> CallbackReturn),
	% Actions :: [Action],
	% MFA :: {atom(), atom(), list()},
	% MFAs :: [MFA],
	Return :: {ok, term()}
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
		| pos_integer().

%---------------------------------------------------------------------
% OPTIONAL: a long argument, used  to configure the parameter, usually
% lower cases words separated by dashes.
% DEFAULT: converted parameter key (e.g. --global.debug)
%---------------------------------------------------------------------
-callback long_argument() -> Return when
	Return :: undefined
		| iolist().

%---------------------------------------------------------------------
% OPTIONAL: the number of element to fetch after the flag.
% DEFAULT: 0
%---------------------------------------------------------------------
-callback elements() -> Return when
	Return :: pos_integer().

%---------------------------------------------------------------------
% OPTIONAL: the type of the value.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback type() -> Return when
	Return :: undefined
		| atom()
		| fun ((term()) -> {ok, term()}).

%---------------------------------------------------------------------
% OPTIONAL: a function to check the value attributed with the key.
%---------------------------------------------------------------------
-callback check(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: ok
		| {error, term()}.

%---------------------------------------------------------------------
% OPTIONAL: a function returning  a string representing an environment
% variable.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback environment() -> Return when
	Return :: undefined
		| binary().

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
% 	Return :: undefined | [atom()|iolist()|tuple()].

%---------------------------------------------------------------------
% @TODO: conflicts callback
% OPTIONAL: defines a list of conflicting parameters
% DEFAULT: undefined
%---------------------------------------------------------------------
% -callback conflicts() -> RETURN when
% 	Return :: undefined | [atom()|iolist()|tuple()].

%---------------------------------------------------------------------
% @TODO: formatter callback
% OPTIONAL: defines a function callback to format short or long
% help message.
% DEFAULT: undefined
%---------------------------------------------------------------------
% -callback formatter(Type, Value) when
% 	Type :: short | long,
% 	Value :: iolist(),
% 	Return :: undefined | {ok, FormattedValue},
% 	FormattedValue :: iolist().

%---------------------------------------------------------------------
% @TODO: positional arguments callback
% OPTIONAL: defines if the argument is positional, those are found
% after a special separator, usually `--'.
%---------------------------------------------------------------------
% -callback positional() -> Return when
% 	Return :: boolean().

%---------------------------------------------------------------------
% @TODO: before/after arguments callback
% OPTIONAL: defines if another argument should be set before or after
% the current one
%---------------------------------------------------------------------
% -callback before() -> Return when
% 	Return :: undefined | [atom()].
% -callback after() -> Return when
% 	Return :: undefined | [atom()].

%---------------------------------------------------------------------
% @TODO: dryrun argument
% OPTIONAL: instead of executing the set callback, it simply returns
% the action and modification would have been applied.
%---------------------------------------------------------------------
% -callback dryrun() -> Return when
% 	Return :: term().

-optional_callbacks([
	runtime/0,
	short_argument/0,
	long_argument/0,
	elements/0,
	type/0,
	check/2,
	environment/0,
	legacy/0,
	short_description/0,
	long_description/0,
	deprecated/0
]).

%%--------------------------------------------------------------------
%% @doc Start `arweave_config' spec server.
%%
%% == Examples ==
%%
%% ```
%% {ok, P} = arweave_config_spec:start_link(arweave_config).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(Module) -> Return when
	Module :: atom(),
	Return :: {ok, pid()}.

start_link(Module) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Module, []).

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
%% @doc
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
%% @doc
%% @end
%%--------------------------------------------------------------------
get_short_argument(ArgumentKey) ->
	todo.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_long_argument(ArgumentKey) ->
	todo.

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
 	{configuration_key, arweave_config_spec_configuration_key},
 	{runtime, arweave_config_spec_runtime},
 	{handle_get, arweave_config_spec_handle_get},
 	{handle_set, arweave_config_spec_handle_set},

	% optional callbacks
	{check, arweave_config_spec_check},
	{deprecated, arweave_config_spec_deprecated},
 	{environment, arweave_config_spec_environment},
 	{short_argument, arweave_config_spec_short_argument},
 	{long_argument, arweave_config_spec_long_argument},
 	{elements, arweave_config_spec_elements},
 	{type, arweave_config_spec_type},
 	{legacy, arweave_config_spec_legacy},
 	{short_description, arweave_config_spec_short_description},
 	{long_description, arweave_config_spec_long_description},
	{default, arweave_config_spec_default}
].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(ModuleSpec) ->
	erlang:process_flag(trap_exit, true),
	Specs = ModuleSpec:spec(),
	{ok, MapSpec} = init_loop(Specs, #{}),
	Ets = ets:new(?MODULE, [named_table, protected]),
	[
		ets:insert(?MODULE, {K, V})
		||
		{K, V} <- maps:to_list(MapSpec)
	],
	?LOG_INFO("~p ready", [?MODULE]),
	{ok, Ets}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_loop([], Buffer) ->
	{ok, Buffer};
init_loop([Module|Rest], Buffer) when is_atom(Module) ->
	case init_module(Module, #{}) of
		{ok, #{ configuration_key := K } = R} ->
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
	?LOG_INFO("loaded module ~p", [Module]),
	{ok, State};
init_module(Module, [{_Callback, ModuleCallback}|Rest], State) ->
	case erlang:apply(ModuleCallback, init, [Module, State]) of
		{ok, NewState} ->
			?LOG_INFO("checked callback ~p:~p", [Module,ModuleCallback]),
			init_module(Module, Rest, NewState);
		Elsewise ->
			?LOG_WARNING("can't load module ~p:~p", [Module, Elsewise]),
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
%% @end
%%--------------------------------------------------------------------
check_type(Parameter, Value, Spec = #{ type := Type }, Buffer) ->
	try
		Result = arweave_config_type:Type(Value),
		check_function(Parameter, Value, Spec, Buffer#{ type => Result })
	catch
		E:R ->
			Error = {E, R},
			check_function(Parameter, Value, Spec, Buffer#{ type => Error })
	end;
check_type(Parameter, Value, Spec, Buffer) ->
	check_function(Parameter, Value, Spec, Buffer#{ type => undefined }).

%%--------------------------------------------------------------------
%% @hidden
%% @doc If a custom check function exists, use it.
%% @end
%%--------------------------------------------------------------------
check_function(Parameter, Value, Spec = #{ check := Check }, Buffer) ->
	try
		Result = Check(Parameter, Value),
		check_final(Parameter, Value, Spec, Buffer#{ check => Result })
	catch
		E:R ->
			Error = {E, R},
			check_final(Parameter, Value, Spec, Buffer#{ check => Error })
	end;
check_function(Parameter, Value, Spec, Buffer) ->
	check_final(Parameter, Value, Spec, Buffer#{ check => undefined }).

%%--------------------------------------------------------------------
%% @hidden
%% @doc final check function, all returned values from the previous
%% call should be present in `Buffer' variable.
%% @end
%%--------------------------------------------------------------------
check_final(_, Value, _, Buffer) ->
	case Buffer of
		#{ type := undefined, check := undefined } ->
			{ok, Value, Buffer};
		#{ type := ok, check := ok } ->
			{ok, Value, Buffer};
		#{ type := undefined, check := ok } ->
			{ok, Value, Buffer};
		#{ type := ok, check := undefined } ->
			{ok, Value, Buffer};
		#{ type := _, check := ok } ->
			{ok, Value, Buffer};
		_ ->
			{error, Value, Buffer}
	end.

%%--------------------------------------------------------------------
%% 1. get he specification, if they are present, we can continue
%% to execute the transaction
%%--------------------------------------------------------------------
apply_set(Parameter, Value) ->
	case spec(Parameter) of
		{ok, Parameter, Spec} ->
			apply_set2(Parameter, Value, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% 2. check if the specification match the parameter/value,
%% if everything is fine, we can continue the execution
%%--------------------------------------------------------------------
apply_set2(Parameter, Value, Spec) ->
	case check(Parameter, Value, Spec) of
		{ok, Return, _} ->
			apply_set3(Parameter, Return, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% 3. let retrieve the value (if set) and use the handle_set/3
%% callback to set the value.
%%--------------------------------------------------------------------
apply_set3(Parameter, Value, Spec = #{ set := Set }) ->
	Default = maps:get(default, Spec, undefined),
	OldValue = arweave_config_store:get(Parameter, Default),
	try Set(Parameter, Value, OldValue) of
		{ok, NewValue} ->
			{ok, NewValue, OldValue};
		{store, NewValue} ->
			apply_set4(Parameter,NewValue,OldValue,Spec);
		Elsewise ->
			Elsewise
	catch
		E:R ->
			{E,R}
	end.

%%--------------------------------------------------------------------
%% 4. the previous callback returned `store', then we store
%% the value into arweave_config_store.
%%--------------------------------------------------------------------
apply_set4(Parameter, NewValue, OldValue, _Spec) ->
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
%%
%%--------------------------------------------------------------------
apply_get(Parameter) ->
	case spec(Parameter) of
		{ok, Parameter, Spec} ->
			apply_get2(Parameter, Spec);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
apply_get2(Parameter, _Spec = #{ default := Default }) ->
	Value = arweave_config_store:get(Parameter, Default),
	{ok, Value};
apply_get2(Parameter, _Spec) ->
	arweave_config_store:get(Parameter).
