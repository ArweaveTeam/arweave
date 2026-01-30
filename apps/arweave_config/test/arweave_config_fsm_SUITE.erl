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
%%% @doc Arweave Configuration Finite State Machine Test Suite.
%%% @end
%%%===================================================================
-module(arweave_config_fsm_SUITE).
-compile(warnings_as_errors).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([default/1]).
-export([
	fsm_callback_ok/1,
	fsm_callback_ok_state/1,
	fsm_callback_function_transition_state/1,
	fsm_callback_module_transition_state/1,
	fsm_callback_error/1,
	fsm_callback_error_state/1,
	fsm_callback_error_wildcard/1,
	fsm_callback_meta/1,
	fsm_callback_ok_meta/1,
	fsm_callback_ok_meta_state/1
]).

-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config_fsm test suite"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_suite(Config) -> Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_suite(_Config) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
	Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		default
	].

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
default(_Config) ->
	ct:pal(test, 1, "set debug"),
	logger:set_module_level(arweave_config_fsm, debug),

	ct:pal(test, 1, "check return without state"),
	{ok, value} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_ok,
			state
		),

	ct:pal(test, 1, "check return with state"),
	{ok, value, state} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_ok_state,
			state
		),

	ct:pal(test, 1, "check function transition with state"),
	{ok, value, #{ state := test, return := ok }} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_function_transition_state,
			#{ state => test }
		),

	ct:pal(test, 1, "check module/function transition with state"),
	{ok, value, #{ state := test, return := ok }} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_module_transition_state,
			#{ state => test }
		),

	ct:pal(test, 1, "check error without state"),
	{error, _} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_error,
			#{ state => test }
		),

	ct:pal(test, 1, "check error with state"),
	{error, _, #{ state := test }} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_error_state,
			#{ state => test }
		),

	ct:pal(test, 1, "check evaluation error"),
	{error, _} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_error_wildcard,
			#{ state => test }
		),

	ct:pal(test, 1, "check evaluation error"),
	{error, _} =
		arweave_config_fsm:init(
			"not_module",
			<<"bad_callback">>,
			#{ state => test }
		),

	ct:pal(test, 1, "return metadata"),
	{meta, Meta1} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_meta,
			#{ meta => true },
			state
		),
	#{ counter := 1 } = Meta1,
	#{ history := _} = Meta1,
	#{ meta := true } = Meta1,

	ct:pal(test, 1, "return metadata with value"),
	{ok, value, Meta2} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_ok_meta,
			#{ meta => true },
			state
		),
	#{ counter := 1 } = Meta2,
	#{ history := _} = Meta2,
	#{ meta := true } = Meta2,

	ct:pal(test, 1, "return metadata with value and state"),
	{ok, value, State3, Meta3} =
		arweave_config_fsm:init(
			?MODULE,
			fsm_callback_ok_meta_state,
			#{ meta => true },
			state
		),
	state = State3,
	#{ counter := 1 } = Meta3,
	#{ history := _} = Meta3,
	#{ meta := true } = Meta3,

	ct:pal(test, 1, "unset debug"),
	logger:set_module_level(arweave_config_fsm, none),

	{comment, "tested"}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_ok(_State) ->
	{ok, value}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_ok_state(State) ->
	{ok, value, State}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_function_transition_state(State) ->
	{next, fsm_callback_ok_state, State#{ return => ok }}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_module_transition_state(State) ->
	{next, ?MODULE, fsm_callback_ok_state, State#{ return => ok }}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_error(_State) ->
	{error, test}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_error_state(State) ->
	{error, test, State#{ return => error }}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_error_wildcard(_State) ->
	{unsupported_return}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_meta(_State) ->
	meta.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_ok_meta(_State) ->
	{ok, value}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
fsm_callback_ok_meta_state(State) ->
	{ok, value, State}.
