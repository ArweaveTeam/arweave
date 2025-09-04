%%%===================================================================
%%% @doc Arweave config specification behavior.
%%%
%%% This module defines the behavior used to configure arweave
%%% parameters.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec).
-export([init/1, required/0, optional/0]).
-include_lib("kernel/include/logger.hrl").

% a configuration key.
-type key() :: [atom() | {atom()}].

% required: define the configuration key used to identify
% arweave parameter, usually stored in a data
% store like ETS.
-callback configuration_key() -> {ok, key()}.

% required: define if the parameter can be set during runtime.
% if true, the parameter can be set when arweave is
% running, else, the parameter can only be set
% during startup
-callback runtime() -> {ok, boolean()}.

% required: define how to retrieve the value using the
% key Key.
-callback handle_get(Key) -> Return when
	Key :: key(),
	Return :: {ok, term()} | {error, term()}.

% required: define how to set the value Value with the key
% Key. It should be transaction.
-callback handle_set(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: {ok, term()} | {error, term()}.

% optional: short argument used to configure the parameter,
% usually a single ASCII letter.
-callback short_argument() -> {ok, pos_integer()}.

% optional: a long argument, used to configure the parameter,
% usually lower cases words separated by dashes
-callback long_argument() -> {ok, [string()]}.

% optional: the number of element to fetch after the flag
-callback elements() -> {ok, pos_integer()}.

% optional: the type of the value
-callback type() -> {ok, atom()}.

% optional: a function to check the value attributed with the key.
-callback check(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: ok | {error, term()}.

% optional: a function returning a string representing an
% environment variable.
-callback environment() -> {ok, string()}.

% optional: a list of legacy references used to previously
% fetch the value.
-callback legacy() -> {ok, [term()]}.

% optional: a short description of the parameter.
-callback short_description() -> {ok, iolist()}.

% optiona: a long description of the parameter.
-callback long_description() -> {ok, iolist()}.

-optional_callbacks([
	short_argument/0,
	long_argument/0,
	elements/0,
	type/0,
	check/2,
	environment/0,
	legacy/0,
	short_description/0,
	long_description/0
]).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
required() -> [
	configuration_key,
	runtime
].

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
optional() -> [
	short_argument,
	long_argument,
	elements,
	type,
	environment,
	legacy,
	short_description,
	long_description
].

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(ModuleSpec) ->
	Specs = ModuleSpec:spec(),
	init_loop(Specs).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init_loop([]) -> ok;
init_loop([Module|Rest]) when is_atom(Module) ->
	{ok, _} = init_module(Module, #{}),
	init_loop(Rest).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init_module(Module, State) ->
	Required = required(),
	init_module_loop_required(Module, Required, State),

	Optional = optional(),
	init_module_loop_optional(Module, Optional, State).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init_module_loop_required(Module, [Function|Rest], State) ->
	try erlang:apply(Module, Function, []) of
		{ok, Return} ->
			NewState = State#{ Function => Return },
			init_module_loop_required(Module, Rest, NewState);
		Elsewise ->
			Elsewise
	catch
		_E:R ->
			{error, R}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init_module_loop_optional(Module, [Function|Rest], State) ->
	try erlang:apply(Module, Function, []) of
		{ok, Return} ->
			NewState = State#{ Function => Return },
			init_module_loop_optional(Module, Rest, NewState);
		Elsewise ->
			Elsewise
	catch
		_:_ ->
			init_module_loop_optional(Module, Rest, State)
	end.
