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
-include_lib("kernel/include/logger.hrl").

% a configuration key.
-type key() :: [atom() | {atom()}].

%---------------------------------------------------------------------
% required:  defines the  configuration key  used to  identify arweave
% parameter, usually stored in a data store like ETS.
%---------------------------------------------------------------------
-callback configuration_key() -> {ok, key()}.

%---------------------------------------------------------------------
% required: defines  if the  parameter can be  set during  runtime. if
% true, the  parameter can be set  when arweave is running,  else, the
% parameter can only be set during startup
%---------------------------------------------------------------------
-callback runtime() -> {ok, boolean()}.

%---------------------------------------------------------------------
% required: defines how to retrieve the value using the key Key.
%---------------------------------------------------------------------
-callback handle_get(Key) -> Return when
	Key :: key(),
	Return :: {ok, term()} | {error, term()}.

%---------------------------------------------------------------------
% required: defines  how to set the  value Value with the  key Key. It
% should be transaction.
%---------------------------------------------------------------------
-callback handle_set(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: {ok, term()} | {error, term()}.

%---------------------------------------------------------------------
% optional: short argument used to  configure the parameter, usually a
% single ASCII letter.
%---------------------------------------------------------------------
-callback short_argument() -> {ok, pos_integer()}.

%---------------------------------------------------------------------
% optional: a long argument, used  to configure the parameter, usually
% lower cases words separated by dashes
%---------------------------------------------------------------------
-callback long_argument() -> {ok, [string()]}.

%---------------------------------------------------------------------
% optional: the number of element to fetch after the flag
%---------------------------------------------------------------------
-callback elements() -> {ok, pos_integer()}.

%---------------------------------------------------------------------
% optional: the type of the value
%---------------------------------------------------------------------
-callback type() -> {ok, atom()}.

%---------------------------------------------------------------------
% optional: a function to check the value attributed with the key.
%---------------------------------------------------------------------
-callback check(Key, Value) -> Return when
	Key :: key(),
	Value :: term(),
	Return :: ok | {error, term()}.

%---------------------------------------------------------------------
% optional: a function returning  a string representing an environment
% variable.
%---------------------------------------------------------------------
-callback environment() -> {ok, string()}.

%---------------------------------------------------------------------
% optional: a list  of legacy references used to  previously fetch the
% value.
%---------------------------------------------------------------------
-callback legacy() -> {ok, [term()]}.

%---------------------------------------------------------------------
% optional: a short description of the parameter.
%---------------------------------------------------------------------
-callback short_description() -> {ok, iolist()}.

%---------------------------------------------------------------------
% optional: a long description of the parameter.
%---------------------------------------------------------------------
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
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
	init_module_required(Module, State).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module_required(Module, State) ->
	Required = ?MODULE:behaviour_info(callbacks) -- ?MODULE:behaviour_info(optional_callbacks),
	{ok, NewState} = init_module_loop_required(Module, Required, State),
	init_module_optional(Module, NewState#{
		get => fun Module:handle_get/1,
		set => fun Module:handle_set/2
	}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module_optional(Module, State) ->
	Optional = ?MODULE:behaviour_info(optional_callbacks),
	init_module_loop_optional(Module, Optional, State).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module_loop_required(_Module, [], State) ->
	{ok, State};
init_module_loop_required(Module, [{Function,0}|Rest], State) ->
	try erlang:apply(Module, Function, []) of
		{ok, Return} ->
			NewState = State#{ Function => Return },
			init_module_loop_required(Module, Rest, NewState);
		Elsewise ->
			Elsewise
	catch
		_E:R ->
			{error, R}
	end;
init_module_loop_required(Module, [_|Rest], State) ->
	init_module_loop_required(Module, Rest, State).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_module_loop_optional(_Module, [], State) ->
	{ok, State};
init_module_loop_optional(Module, [{Function,0}|Rest], State) ->
	try erlang:apply(Module, Function, []) of
		{ok, Return} ->
			NewState = State#{ Function => Return },
			init_module_loop_optional(Module, Rest, NewState);
		Elsewise ->
			Elsewise
	catch
		_:_ ->
			init_module_loop_optional(Module, Rest, State)
	end;
init_module_loop_optional(Module, [_|Rest], State) ->
	init_module_loop_optional(Module, Rest, State).

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
