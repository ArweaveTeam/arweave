%%%===================================================================
%%% @doc Arweave config specification behavior.
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
	  Return :: {ok, key()}.

%---------------------------------------------------------------------
% REQUIRED: defines  if the  parameter can be  set during  runtime. if
% true, the  parameter can be set  when arweave is running,  else, the
% parameter can only be set during startup
%---------------------------------------------------------------------
-callback runtime() -> Return when
	  Return :: {ok, boolean()}.

%---------------------------------------------------------------------
% REQUIRED: defines how to retrieve the value using the key Key.
%---------------------------------------------------------------------
-callback handle_get(Key) -> Return when
	Key :: key(),
	Return :: handle_get_ok_return() | error_return().

%---------------------------------------------------------------------
% REQUIRED: defines  how to set the  value Value with the  key Key. It
% should be transaction.
%---------------------------------------------------------------------
-callback handle_set(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: handle_set_ok_return() | error_return().

%---------------------------------------------------------------------
% OPTIONAL: short argument used to  configure the parameter, usually a
% single ASCII letter.
%---------------------------------------------------------------------
-callback short_argument() -> Return when
	  Return :: {ok, pos_integer()}.

%---------------------------------------------------------------------
% OPTIONAL: a long argument, used  to configure the parameter, usually
% lower cases words separated by dashes.
%---------------------------------------------------------------------
-callback long_argument() -> Return when
	  Return :: {ok, [string()]}.

%---------------------------------------------------------------------
% OPTIONAL: the number of element to fetch after the flag.
%---------------------------------------------------------------------
-callback elements() -> Return when
	  Return :: {ok, pos_integer()}.

%---------------------------------------------------------------------
% OPTIONAL: the type of the value.
%---------------------------------------------------------------------
-callback type() -> Return when
	  Return :: {ok, atom()}.

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
%---------------------------------------------------------------------
-callback environment() -> Return when
	Return :: {ok, string()}.

%---------------------------------------------------------------------
% OPTIONAL: a list  of legacy references used to  previously fetch the
% value.
%---------------------------------------------------------------------
-callback legacy() -> Return when
	Return :: {ok, [term()]}.

%---------------------------------------------------------------------
% OPTIONAL: a short description of the parameter.
%---------------------------------------------------------------------
-callback short_description() -> Return when
	Return :: {ok, iolist()}.

%---------------------------------------------------------------------
% OPTIONAL: a long description of the parameter.
%---------------------------------------------------------------------
-callback long_description() -> Return when
	Return :: {ok, iolist()}.

%---------------------------------------------------------------------
% OPTIONAL: defines if a parameter is deprecated, can eventually
% returns a message.
%---------------------------------------------------------------------
-callback deprecated() -> Return when
	Return :: true | {true, term()} | false.

-optional_callbacks([
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
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
callbacks_check() -> [
 	{configuration_key, arweave_config_spec_configuration_key},
	{deprecated, arweave_config_spec_deprecated}
 	% {runtime, arweave_config_spec_runtime},
 	% {handle_set, arweave_config_spec_handle_set},
 	% {handle_get, arweave_config_spec_handle_set}
% 	{short_argument, arweave_config_spec_short_argument},
% 	{long_argument, arweave_config_spec_long_argument},
% 	{elements, arweave_config_spec_elements},
% 	{type, arweave_config_spec_type},
% 	{environment, arweave_config_spec_environment},
% 	{legacy, arweave_config_spec_legacy},
% 	{short_description, arweave_config_spec_short_description},
% 	{long_description, arweave_config_spec_long_description},
].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(ModuleSpec) ->
	erlang:process_flag(trap_exit, true),
	Specs = ModuleSpec:spec(),
	{ok, State} = init_loop(Specs, #{}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_loop([], Buffer) -> {ok, Buffer};
init_loop([Module|Rest], Buffer) when is_atom(Module) ->
	{ok, #{ configuration_key := K } = R} = init_module(Module, #{}),
	init_loop(Rest, Buffer#{ K => R }).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module(Module, State) ->
	init_module(Module, callbacks_check(), State).

init_module(Module, [], State) ->
	{ok, State};
init_module(Module, [{Callback, ModuleCallback}|Rest], State) ->
	case erlang:apply(ModuleCallback, init, [Module, State]) of
		{ok, NewState} ->
			init_module(Module, Rest, NewState);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call({check, Key, Value}, From, State) ->
	Return = wip,
	{reply, Return, State};
handle_call({get, Key}, From, State) ->
	Value = maps:get(Key, State),
	{reply, {ok, Value}, State};
handle_call(Msg, From, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
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
