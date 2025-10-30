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
	ArweaveEnvironment = arweave_config_environment:get(),

	% let check if all variables we have configured are present
	[
	 	begin
			VE = proplists:get_value(list_to_binary(K), ArweaveEnvironment),
			VE = list_to_binary(V)
		end
		|| {K, V} <- Environment
	],

	% let check them one by one.
	[
		begin
			{ok, VE} = arweave_config_environment:get(list_to_binary(K)),
			VE = list_to_binary(V)
		end
		|| {K, V} <- Environment
	],

	% load the environment, it will lookup in the environment list
	% and set the value, in our case, AR_DEBUG should be
	% configured and set to true instead of false.
	{ok, false} = arweave_config:get([debug]),
	ok = arweave_config_environment:load(),
	{ok, true} = arweave_config:get([debug]).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
environment() ->
	[{"AR_TEST_ENVIRONMENT_VARIABLE", "test"}
	,{"AR_DEBUG", "true"}
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
set_environment() ->
	[ os:putenv(K,V) || {K,V} <- environment() ].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
unset_environment() ->
	[ os:unsetenv(K) || {K,_} <- environment() ].
