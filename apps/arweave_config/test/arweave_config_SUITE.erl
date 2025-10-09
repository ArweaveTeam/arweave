%%%===================================================================
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc
%%% @end
%%%
%%% ------------------------------------------------------------------
%%%
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%===================================================================
-module(arweave_config_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([arweave_config/1]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config test main interface"}.

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
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[ arweave_config ].

%%--------------------------------------------------------------------
%% @doc test `arweave_config' main interface.
%% @end
%%--------------------------------------------------------------------
arweave_config(_Config) ->
	% legacy compatible interface, to remove
	ct:pal(test, 1, "init legacy environment"),
	ok = arweave_config:set_env(#config{}),

	% legacy compatible interface, to remove
	ct:pal(test, 1, "get legacy environment"),
	{ok, Config1} = arweave_config:get_env(),
	false = Config1#config.init,

	% legacy compatible interface, to remove
	ct:pal(test, 1, "set legacy environment"),
	ok = arweave_config:set_env(#config{ init = true }),
	{ok, Config2} = arweave_config:get_env(),
	true = Config2#config.init,

	{comment, "arweave_config interface tested"}.
