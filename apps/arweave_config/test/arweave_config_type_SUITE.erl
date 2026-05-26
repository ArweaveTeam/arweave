%%% @doc
-module(arweave_config_type_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok.

all() ->
	[
		atom,
		integer,
		boolean,
		pos_integer,
		ipv4,
		path,
		tcp_port,
		file,
		logging_template,
		peer_id_ip_only,
		peer_id_ip_with_port,
		peer_id_hostname_only,
		peer_id_hostname_with_port,
		peer_id_ipv4_tuple,
		peer_id_ipv6_bracketed,
		peer_id_default_port_collapses,
		peer_id_distinct_ports_stay_distinct,
		peer_id_invalid
	].

%%====================================================================
%% Test cases
%%====================================================================

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

tcp_port(_Config) ->
	{ok, 0} = arweave_config_type:tcp_port(0),
	{ok, 65535} = arweave_config_type:tcp_port(65535),
	{ok, 1234} = arweave_config_type:tcp_port(1234),
	{ok, 1234} = arweave_config_type:tcp_port("1234"),
	{ok, 1234} = arweave_config_type:tcp_port(<<"1234">>),
	{error, 78912} = arweave_config_type:tcp_port(<<"78912">>).

file(_Config) ->
	{ok, <<"/tmp/arweave.sock">>} =
		arweave_config_type:file(<<"/tmp/arweave.sock">>),

	{ok, P1} =
		arweave_config_type:file("./arweave.sock"),
	true = is_binary(P1),

	{error, _} =
		arweave_config_type:file("/random/t/a/b/c.sock"),

	{error, _} =
		arweave_config_type:file("/root/data/arweave.sock"),

	{error, _} =
		arweave_config_type:file(1234),

	ok.

logging_template(_Config) ->
	{ok, ["test", "\n"]} =
		arweave_config_type:logging_template("test"),

	{ok, ["test","\n"]} =
		arweave_config_type:logging_template(<<"test">>),

	{ok, [test,"\n"]} =
		arweave_config_type:logging_template(<<"%test">>),

	{ok, ["message:", " ", test, "\n"]} =
		arweave_config_type:logging_template("message: %test"),

	{ok, ["message:%test","\n"]} =
		arweave_config_type:logging_template("message:%test"),

	{error, _} =
		arweave_config_type:logging_template("%test!#&"),

	{error, _} =
		arweave_config_type:logging_template("%total_random_atom"),

	ok.

peer_id_ip_only(_Config) ->
	?assertEqual({ok, <<"1.2.3.4:1984">>},
		arweave_config_type:peer_id(<<"1.2.3.4">>)),
	?assertEqual({ok, <<"1.2.3.4:1984">>},
		arweave_config_type:peer_id("1.2.3.4")).

peer_id_ip_with_port(_Config) ->
	?assertEqual({ok, <<"1.2.3.4:1984">>},
		arweave_config_type:peer_id(<<"1.2.3.4:1984">>)),
	?assertEqual({ok, <<"1.2.3.4:9999">>},
		arweave_config_type:peer_id(<<"1.2.3.4:9999">>)).

peer_id_hostname_only(_Config) ->
	?assertEqual({ok, <<"myhost:1984">>},
		arweave_config_type:peer_id(<<"myhost">>)),
	?assertEqual({ok, <<"my-host.example.com:1984">>},
		arweave_config_type:peer_id(<<"my-host.example.com">>)).

peer_id_hostname_with_port(_Config) ->
	?assertEqual({ok, <<"myhost:1984">>},
		arweave_config_type:peer_id(<<"myhost:1984">>)),
	?assertEqual({ok, <<"myhost:8080">>},
		arweave_config_type:peer_id(<<"myhost:8080">>)).

peer_id_ipv4_tuple(_Config) ->
	?assertEqual({ok, <<"1.2.3.4:1984">>},
		arweave_config_type:peer_id({1, 2, 3, 4})),
	?assertEqual({ok, <<"1.2.3.4:9999">>},
		arweave_config_type:peer_id({1, 2, 3, 4, 9999})).

peer_id_ipv6_bracketed(_Config) ->
	?assertEqual({ok, <<"[::1]:1984">>},
		arweave_config_type:peer_id(<<"[::1]">>)),
	?assertEqual({ok, <<"[::1]:9999">>},
		arweave_config_type:peer_id(<<"[::1]:9999">>)).

%% Omitting the port yields the default-port form of the same peer.
peer_id_default_port_collapses(_Config) ->
	{ok, A} = arweave_config_type:peer_id(<<"1.2.3.4">>),
	{ok, B} = arweave_config_type:peer_id(<<"1.2.3.4:1984">>),
	?assertEqual(A, B).

peer_id_distinct_ports_stay_distinct(_Config) ->
	{ok, A} = arweave_config_type:peer_id(<<"1.2.3.4:1984">>),
	{ok, B} = arweave_config_type:peer_id(<<"1.2.3.4:1985">>),
	?assertNotEqual(A, B).

peer_id_invalid(_Config) ->
	?assertMatch({error, _}, arweave_config_type:peer_id(<<>>)),
	?assertMatch({error, _}, arweave_config_type:peer_id(<<"1.2.3.4:bad_port">>)),
	?assertMatch({error, _}, arweave_config_type:peer_id(<<"host with space">>)),
	?assertMatch({error, _}, arweave_config_type:peer_id(<<"1.2.3.4:99999">>)),
	?assertMatch({error, _}, arweave_config_type:peer_id(<<"1.2.3.4:-1">>)),
	?assertMatch({error, _}, arweave_config_type:peer_id(123)).
