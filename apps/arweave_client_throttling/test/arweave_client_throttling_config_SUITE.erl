%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Tests for `arweave_client_throttling_config'.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_config_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	defaults_when_unset/1,
	custom_groups_from_env/1,
	normalize_fills_missing_keys/1,
	get_value/1,
	missing_group_returns_error/1
]).

-include_lib("common_test/include/ct.hrl").

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 10}}].

description() ->
	{description, "arweave_client_throttling_config"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	application:unset_env(arweave_client_throttling, groups),
	Config.

end_per_testcase(_TestCase, _Config) ->
	application:unset_env(arweave_client_throttling, groups),
	ok.

all() ->
	[
		defaults_when_unset,
		custom_groups_from_env,
		normalize_fills_missing_keys,
		get_value,
		missing_group_returns_error
	].

%%--------------------------------------------------------------------
%% @doc When no `groups' env var is set, the built-in defaults are
%% returned (`general' and `data_sync_record').
%% @end
%%--------------------------------------------------------------------
defaults_when_unset(_Config) ->
	Groups = arweave_client_throttling_config:get_groups(),
	Ids = [Id || #{id := Id} <- Groups],
	true = lists:member(general, Ids),
	true = lists:member(data_sync_record, Ids),
	ok.

%%--------------------------------------------------------------------
%% @doc A user-provided `groups' env var fully overrides the defaults.
%% @end
%%--------------------------------------------------------------------
custom_groups_from_env(_Config) ->
	application:set_env(arweave_client_throttling, groups,
		[#{id => only_one, initial_remaining => 5}]),
	[Group] = arweave_client_throttling_config:get_groups(),
	#{id := only_one, initial_remaining := 5} = Group,
	ok.

%%--------------------------------------------------------------------
%% @doc `normalize_group/1' fills missing keys with their defaults
%% without overwriting explicit values.
%% @end
%%--------------------------------------------------------------------
normalize_fills_missing_keys(_Config) ->
	N = arweave_client_throttling_config:normalize_group(
		#{id => x, initial_remaining => 99}),
	#{
		id := x,
		initial_remaining := 99,
		max_queue_length := MaxLen,
		concurrency_window_ms := Window
	} = N,
	true = is_integer(MaxLen) andalso MaxLen > 0,
	true = is_integer(Window) andalso Window >= 0,
	ok.

%%--------------------------------------------------------------------
%% @doc `get_value/2' returns either a value or a structured error.
%% @end
%%--------------------------------------------------------------------
get_value(_Config) ->
	{ok, _} =
		arweave_client_throttling_config:get_value(general, initial_remaining),
	{error, {key_not_found, general, no_such_key}} =
		arweave_client_throttling_config:get_value(general, no_such_key),
	ok.

%%--------------------------------------------------------------------
%% @doc Asking for a non-existing group yields `{error, not_found}'.
%% @end
%%--------------------------------------------------------------------
missing_group_returns_error(_Config) ->
	{error, not_found} =
		arweave_client_throttling_config:get_group(does_not_exist),
	{error, not_found} =
		arweave_client_throttling_config:get_value(does_not_exist, anything),
	ok.
