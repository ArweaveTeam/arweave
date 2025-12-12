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
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_environment_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([default/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave config environment interface"}.

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
	set_environment(),
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	[{environment, environment()}|Config].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop(),
	unset_environment().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[ default ].

%%--------------------------------------------------------------------
%% @doc test `arweave_config_environment' main interface.
%% @end
%%--------------------------------------------------------------------
default(Config) ->
	Environment = proplists:get_value(environment, Config),

	ct:pal(test, 1, "ensure arweave_config_environment is started"),
	true = is_process_alive(whereis(arweave_config_environment)),

	ct:pal(test, 1, "send an unsupported message to the process"),
	ok = gen_server:call(arweave_config_environment, '@random_test', 1000),
	ok = gen_server:cast(arweave_config_environment, '@random_test'),
	_ = erlang:send(arweave_config_environment, '@random_test'),

	ct:pal(test, 1, "reset arweave_config_environment"),
	arweave_config_environment:reset(),

	ct:pal(test, 1, "retrieve environment from arweave_config_environment"),
	ArweaveEnvironment = arweave_config_environment:get(),

	ct:pal(test, 1, "check if variables have been configured"),
	[
	 	begin
			ct:pal(test, 1, "found: ~p", [{K,V}]),
			BK = list_to_binary(K),
			VE = proplists:get_value(BK, ArweaveEnvironment),
			ct:pal(test, 1, "~p", [{BK,VE}]),
			VE = list_to_binary(V)
		end
		|| {K, V} <- Environment
	],

	ct:pal(test, 1, "check all variables one by one"),
	[
		begin
			ct:pal(test, 1, "check: ~p", [{K,V}]),
			BK = list_to_binary(K),
			{ok, VE} = arweave_config_environment:get(BK),
			VE = list_to_binary(V)
		end
		|| {K, V} <- Environment
	],

	ct:pal(test, 1, "check current value of debug parameter"),
	{ok, false} = arweave_config:get([debug]),
	#config{ debug = false } = arweave_config_legacy:get(),

	% load the environment, it will lookup in the environment list
	% and set the value, in our case, AR_DEBUG should be
	% configured and set to true instead of false.
	ct:pal(test, 1, "load the environment"),
	ok = arweave_config_environment:load(),

	ct:pal(test, 1, "check [debug] parameter."),
	{ok, true} = arweave_config:get([debug]),

	ct:pal(test, 1, "check [debug] parameter (legacy)."),
	#config{ debug = true } = arweave_config_legacy:get(),

	ct:pal(test, 1, "check unconfigured environment variable"),
	{error, not_found} =
		arweave_config_environment:get(<<"UNKNOWN_VARIABLE">>),

	{comment, "environment feature tested"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
environment() ->
	[
		{"AR_TEST_ENVIRONMENT_VARIABLE", "test"},
		{"AR_DEBUG", "true"}
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
set_environment() ->
	[
		begin
			ct:pal(test, 1, "prepare: set ~p=~p",[K,V]),
			os:putenv(K,V),
			V = os:getenv(K)
		end
		|| {K,V} <- environment()
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
unset_environment() ->
	[
		begin
			ct:pal(test, 1, "cleanup: unset ~p", [K]),
			os:unsetenv(K)
		end
		|| {K,_} <- environment()
	].
