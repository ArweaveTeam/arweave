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
-module(arweave_config_legacy_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([arweave_config_legacy/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config_legacy test"}.

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
	ct:pal(info, 1, "start arweave_config_legacy"),
	{ok, Pid} = arweave_config_legacy:start_link(),
	[{arweave_config_legacy, Pid}|Config].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config_legacy"),
	ok = arweave_config_legacy:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[ arweave_config_legacy ].

%%--------------------------------------------------------------------
%% @doc test `arweave_config' main interface.
%% @end
%%--------------------------------------------------------------------
arweave_config_legacy(_Config) ->
	ct:pal(test, 1, "config keys should be the same"),
	Keys = arweave_config_legacy:keys(),
	ConfigKeys = record_info(fields, config),
	LengthKeys = length(Keys),
	LengthConfigKeys = length(ConfigKeys),
	LengthKeys = LengthConfigKeys,
	[ K1 = K2 || {K1, K2} <- lists:zip(Keys, ConfigKeys) ],

	ct:pal(test, 1, "check if config keys are present"),
	[
		true = arweave_config_legacy:has_key(Key)
	||
		Key <- ConfigKeys
	],

	ct:pal(test, 1, "all values should be set with default"),
	#config{} = arweave_config_legacy:get(),
	[
		begin
			{ok, VC} = arweave_config_legacy:get_config_value(Key, #config{}),
			VP = arweave_config_legacy:get(Key),
			VC = VP
		end
	||
		Key <- ConfigKeys
	],

	ct:pal(test, 1, "set init value to true"),
	arweave_config_legacy:set(init, true),
	true = arweave_config_legacy:get(init),

	ct:pal(test, 1, "reset the configuration to default value"),
	arweave_config_legacy:reset(),
	false = arweave_config_legacy:get(init),

	ct:pal(test, 1, "check legacy application env interface"),
	arweave_config_legacy:set_env(#config{ init = true}),
	{ok, #config{ init = true }} = arweave_config_legacy:get_env(),
	{ok, #config{ init = true }} = application:get_env(arweave_config, config),

	{comment, "arweave_config_legacy tested"}.
