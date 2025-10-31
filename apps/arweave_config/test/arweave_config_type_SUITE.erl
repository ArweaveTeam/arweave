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
-module(arweave_config_type_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	none/1,
	any/1,
	boolean/1,
	atom/1,
	integer/1,
	pos_integer/1,
	ipv4/1,
	path/1,
	base64/1,
	base64url/1,
	tcp_port/1,
	file/1
]).
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, "arweave_config_type test interface"}.

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
		none,
		any,
		atom,
		integer,
		boolean,
		integer,
		pos_integer,
		ipv4,
		path,
		base64,
		base64url,
		tcp_port,
		file
	].

none(_Config) ->
	{error, 1} = arweave_config_type:none(1).

any(_Config) ->
	{ok, 1} = arweave_config_type:any(1).

atom(_Config) ->
	{ok, atom} = arweave_config_type:atom(atom),
	{ok, atom} = arweave_config_type:atom(<<"atom">>),
	{ok, atom} = arweave_config_type:atom("atom").

boolean(_Config) ->
	[
		{ok, true} = arweave_config_type:boolean(X)
		|| X <- [<<"true">>, "true", true]
	],
	[
		{ok, false} = arweave_config_type:boolean(X)
		|| X <- [<<"false">>, "false", false]
	],
	{error, not_boolean} =
		arweave_config_type:boolean(not_boolean).

integer(_Config) ->
	{ok, 1} = arweave_config_type:integer(1),
	{ok, 1} = arweave_config_type:integer("1"),
	{ok, 1} = arweave_config_type:integer(<<"1">>),
	{error, a} = arweave_config_type:integer(a).

pos_integer(_Config) ->
	{ok, 1} = arweave_config_type:pos_integer(1),
	{error, -1} = arweave_config_type:pos_integer(-1).

ipv4(_Config) ->
	{ok, <<"127.0.0.1">>} = arweave_config_type:ipv4("127.0.0.1"),
	{ok, <<"127.0.0.1">>} = arweave_config_type:ipv4({127,0,0,1}),
	{ok, <<"127.0.0.1">>} = arweave_config_type:ipv4(<<"127.0.0.1">>),
	{error, _ } = arweave_config_type:ipv4(test).

path(Config) ->
	_PrivDir = proplists:get_value(priv_dir, Config),
	{ok, Cwd} = file:get_cwd(),

	% absolute path
	{ok, <<"/">>} = arweave_config_type:path(<<"/">>),
	{ok, <<"/">>} = arweave_config_type:path("/"),

	% relative path: convert automatically in absolute path
	CwdBinary = list_to_binary(Cwd),
	{ok, CwdBinary} = arweave_config_type:path(<<"./">>),
	{ok, CwdBinary} = arweave_config_type:path("./").

base64(_Config) ->
	{ok, <<"test">>} = arweave_config_type:base64("dGVzdA=="),
	{ok, <<"test">>} = arweave_config_type:base64(<<"dGVzdA==">>).

base64url(_Config) ->
	{ok, <<"test">>} = arweave_config_type:base64url("dGVzdA"),
	{ok, <<"test">>} = arweave_config_type:base64url(<<"dGVzdA">>).

tcp_port(_Config) ->
	{ok, 0} = arweave_config_type:tcp_port(0),
	{ok, 65535} = arweave_config_type:tcp_port(65535),
	{ok, 1234} = arweave_config_type:tcp_port(1234),
	{ok, 1234} = arweave_config_type:tcp_port("1234"),
	{ok, 1234} = arweave_config_type:tcp_port(<<"1234">>),
	{error, 78912} = arweave_config_type:tcp_port(<<"78912">>).

file(_Config) ->
	ct:pal(test, 1, "test absolute path and path as binary"),
	{ok, <<"/tmp/arweave.sock">>} =
		arweave_config_type:file(<<"/tmp/arweave.sock">>),

	ct:pal(test, 1, "test relative path and path as list"),
	{ok, P1} =
		arweave_config_type:file("./arweave.sock"),
	true = is_binary(P1),

	ct:pal(test, 1, "test a wrong path"),
	{error, _} =
		arweave_config_type:file("/random/t/a/b/c.sock"),

	ct:pal(test, 1, "test a file without write access"),
	{error, _} =
		arweave_config_type:file("/root/data/arweave.sock"),

	ct:pal(test, 1, "test a wrong erlang type"),
	{error, _} =
		arweave_config_type:file(1234),

	ok.
