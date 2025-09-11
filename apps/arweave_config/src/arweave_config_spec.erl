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
%%% @end
%%%===================================================================
-module(arweave_config_spec).
-behavior(gen_server).
-export([get/1, set/1]).
-export([start_link/1]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([is_function_exported/3]).
-include_lib("kernel/include/logger.hrl").

% a configuration key.
-type key() :: [atom() | {atom()}].
-type error_message() :: #{ reason => term() }.
-type error_return() :: {error, error_message()}.
-type handle_get_ok_return() :: {ok, CurrentValue :: term()}.
-type handle_set_ok_return() :: {ok, NewValue :: term(), PreviousValue :: term()}.

%---------------------------------------------------------------------
% REQUIRED:  defines the  configuration key  used to  identify arweave
% parameter, usually stored in a data store like ETS.
%---------------------------------------------------------------------
-callback configuration_key() -> Return when
	Return :: [atom() | iolist() | tuple()].

%---------------------------------------------------------------------
% REQUIRED: defines how to retrieve the value using the parameter key.
%---------------------------------------------------------------------
-callback handle_get(Key) -> Return when
	Key :: key(),
	Return :: handle_get_ok_return() | error_return().

%---------------------------------------------------------------------
% REQUIRED: defines how to set the value Value with the parameter key.
% It should be transaction.
%---------------------------------------------------------------------
-callback handle_set(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: handle_set_ok_return() | error_return().

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
	Return :: undefined | pos_integer().

%---------------------------------------------------------------------
% OPTIONAL: a long argument, used  to configure the parameter, usually
% lower cases words separated by dashes.
% DEFAULT: converted parameter key (e.g. --global.debug)
%---------------------------------------------------------------------
-callback long_argument() -> Return when
	Return :: undefined | iolist().

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
	Return :: undefined | atom().

%---------------------------------------------------------------------
% OPTIONAL: a function to check the value attributed with the key.
%---------------------------------------------------------------------
-callback check(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: ok | error_return().

%---------------------------------------------------------------------
% OPTIONAL: a function returning  a string representing an environment
% variable.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback environment() -> Return when
	Return :: undefined | [string()].

%---------------------------------------------------------------------
% OPTIONAL: a list  of legacy references used to  previously fetch the
% value.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback legacy() -> Return when
	Return :: undefined | [atom() | iolist()].

%---------------------------------------------------------------------
% OPTIONAL: a short description of the parameter.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback short_description() -> Return when
	Return :: undefined | iolist().

%---------------------------------------------------------------------
% OPTIONAL: a long description of the parameter.
% DEFAULT: undefined
%---------------------------------------------------------------------
-callback long_description() -> Return when
	Return :: undefined | iolist().

%---------------------------------------------------------------------
% OPTIONAL: defines if a parameter is deprecated, can eventually
% returns a message.
% DEFAULT: false
%---------------------------------------------------------------------
-callback deprecated() -> Return when
	Return :: true | {true, term()} | false.

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
%% @doc get a specification.
%% @end
%%--------------------------------------------------------------------
get(Spec) ->
	gen_server:call(?MODULE, {get, Spec}, 10_000).

%%--------------------------------------------------------------------
%% @doc set a parameters. The process will be in charge to check both
%% keys and values then if everything is good, it will execute a side
%% effect (to modify the application state) and finally store/update
%% the value in `arweave_config_store'.
%%
%% == Examples ==
%%
%% A parameter is already parsed and should be valid. Mostly used
%% internally.
%%
%% ```
%% set({parameter, [global,debug], true}).
%% '''
%%
%% An environment is made of a key (usually in uppercase) prefixed by
%% `AR_' and followed by a value. both values are not parsed and
%% sanitized.
%%
%% ```
%% set({environment, <<"AR_DEBUG">>, <<"true">>}).
%% set({environment, <<"AR_DEBUG">>, <<"false">>}).
%% '''
%%
%% An argument format is usually a binary or a list of binaries. Both
%% values are not parsed nor sanitized.
%%
%% ```
%% set({argument, <<"-d">>, <<"true">>}).
%% set({argument, <<"--global.debug">>, <<"true">>}).
%% set({argument, <<"--global.debug">>, <<"false">>}).
%% set({argument, <<"--global.test">>, [<<"list">>, <<"of">>, <<"value">>]}).
%% '''
%%
%% A configuration is usually a parsed JSON, YAML or TOML file using a
%% map datastructure.
%%
%% ```
%% set({config, #{ <<"global">> => #{ <<"debug">> => true }}).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
set({parameter, Key, Value}) ->
	gen_server:call(?MODULE, {set, parameter, Key, Value});
set({environment, Key, Value}) ->
	gen_server:call(
		?MODULE,
		{set, environment, Key, Value}
	);
set({argument, Key, Value}) ->
	gen_server:call(?MODULE, {set, argument, Key, Value});
set({config, Config}) ->
	gen_server:call(?MODULE, {set, config, Config}).

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
 	{long_description, arweave_config_spec_long_description}
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
handle_call({set, environment, Key, Value}, _From, State) ->
	Pattern = {'$1', #{ environment => '$2' }},
	Guard = [{'=:=', '$2', Key}],
	Select = [{{'$1', '$2'}}],
	case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
		I = [{Param, Key}] ->
			io:format("~p~n", [I]);
		_ ->
			ok
	end,
	% 1. check if an environment key is present in the spec
	%   true = environment(Key),
	% 2. ensure the specification is good for the key/value
	%   {ok, Value} = check(Key, Value),
	% 3. if valid, store the key/value in arweave_store
	%   arweave_config_store:set(Key, Value)
	{reply, ok, State};
handle_call({set, argument, Key, Value}, _From, State) ->
	% 1. the arguments are usually passed to the module entry
	%    point, in the case of arweave, this is ar module. We need
	%    a way to retrieve those arguments, a switch will then be
	%    required on ar module
	% 2. parse the arguments and check if they are present in the
	%    specifications
	% 3. if it's good, store the key/value in the configuration
	%    store
	{reply, ok, State};
handle_call({set, config, Value}, _From, State) ->
	% 1. the configuration received should be a map, if not, it
	%    should fail.
	{reply, ok, State};
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
