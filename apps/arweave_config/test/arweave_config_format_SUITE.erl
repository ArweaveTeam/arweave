%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Config Format Test Suite.
%%% @end
%%%===================================================================
-module(arweave_config_format_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	json/1,
	toml/1,
	yaml/1,
	legacy/1
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
description() -> {description, "arweave_config format"}.

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
	[
		json,
		toml,
		yaml,
		legacy
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
json(_Config) ->
	{ok, #{}} = arweave_config_format_json:parse(""),
	{ok, #{}} = arweave_config_format_json:parse(<<"">>),
	{ok, #{}} = arweave_config_format_json:parse(<<"{}">>),
	{ok, #{}} = arweave_config_format_json:parse("{}"),
	{error, _} = arweave_config_format_json:parse("--"),
	{comment, "tested json format"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
toml(_Config) ->
	{ok, #{}} = arweave_config_format_toml:parse(""),
	{ok, #{}} = arweave_config_format_toml:parse(<<"">>),
	{ok, #{}} = arweave_config_format_toml:parse(<<"test = 1">>),
	{ok, #{}} = arweave_config_format_toml:parse("test = 1"),
	{error, _} = arweave_config_format_toml:parse("--[]"),
	{comment, "tested toml format"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
yaml(_Config) ->
	{ok, #{}} = arweave_config_format_yaml:parse(""),
	{ok, #{}} = arweave_config_format_yaml:parse(<<"">>),
	{ok, _} = arweave_config_format_yaml:parse(<<"test: 1\n">>),
	{ok, _} = arweave_config_format_yaml:parse("test: 1\n"),
	{error, _} = arweave_config_format_yaml:parse("--[]"),
	{error, _} = arweave_config_format_yaml:parse(<<"---\n---\n">>),
	{comment, "tested yaml format"}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
legacy(_Config) ->
	{ok, _} = arweave_config_format_legacy:parse(""),
	{ok, _} = arweave_config_format_legacy:parse(<<"">>),
	{ok, _} = arweave_config_format_legacy:parse(<<"{}">>),
	{ok, _} = arweave_config_format_legacy:parse("{}"),
	{error, _} = arweave_config_format_legacy:parse("--"),
	{comment, "tested legacy format"}.
