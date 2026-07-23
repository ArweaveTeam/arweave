-module(arweave_config_peer_parity_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Hostname resolution is mocked so the parity checks are hermetic (no
%% DNS): `myhost[:Port]' resolves to {1,2,3,4,Port}. Everything else
%% (IPv4 literals) passes through to the real parser.
suite() ->
    [{timetrap, {seconds, 60}}].

init_per_testcase(_TestCase, Config) ->
    ok = arweave_config:start(),
    meck:new(ar_util, [passthrough]),
    meck:expect(ar_util, safe_parse_peer, fun mock_safe_parse_peer/1),
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(ar_util),
    ok = arweave_config:stop().

all() ->
    [
        ip_parity_across_formats,
        ip_without_port_parity_across_formats,
        hostname_parity_across_formats,
        hostname_without_port_parity_across_formats
    ].

%%====================================================================
%% Test cases
%%====================================================================

%% An IPv4 literal resolves to the same `{A,B,C,D,Port}' tuple for every
%% option through every format.
ip_parity_across_formats(_Config) ->
    assert_parity(ip).

%% A port-less IPv4 literal resolves to the same default-port tuple in
%% every format.
ip_without_port_parity_across_formats(_Config) ->
    assert_parity(ip_no_port).

%% A hostname must follow the legacy resolution logic in every format:
%% eager options (trusted/block_gossip/local/cm_peer/cm_exit) resolve to an
%% IP tuple at parse time; VDF options keep the `<<"host:port">>' binary so
%% the runtime resolver can re-resolve them after DNS changes.
hostname_parity_across_formats(_Config) ->
    assert_parity(hostname).

%% A port-less hostname follows the same per-option rule, with the default
%% port applied.
hostname_without_port_parity_across_formats(_Config) ->
    assert_parity(hostname_no_port).

%%====================================================================
%% Helpers
%%====================================================================

assert_parity(InputKind) ->
    Value = value_str(InputKind),
    lists:foreach(
        fun(OptionSpec) ->
            Expected = expected(OptionSpec, InputKind),
            lists:foreach(
                fun(Format) ->
                    Actual = store_value(Format, OptionSpec, Value),
                    Option = maps:get(option, OptionSpec),
                    ?assertEqual(
                        {Option, Format, Expected},
                        {Option, Format, Actual})
                end,
                [legacy_cli, cli, legacy_json, json, yaml])
        end,
        option_specs()).

%% Every option that accepts a peer, with its spelling in each format.
%% `list' distinguishes list-valued options from the `cm_exit' singleton;
%% `eager' marks the options legacy resolves at parse time.
option_specs() ->
    [
        #{option => trusted, legacy_cli => "peer",
            legacy_json => <<"peers">>, list => true, eager => true},
        #{option => block_gossip, legacy_cli => "block_gossip_peer",
            legacy_json => <<"block_gossip_peers">>, list => true,
            eager => true},
        #{option => local, legacy_cli => "local_peer",
            legacy_json => <<"local_peers">>, list => true, eager => true},
        #{option => cm_peer, legacy_cli => "cm_peer",
            legacy_json => <<"cm_peers">>, list => true, eager => true},
        #{option => cm_exit, legacy_cli => "cm_exit_peer",
            legacy_json => <<"cm_exit_peer">>, list => false, eager => true},
        #{option => vdf_client, legacy_cli => "vdf_client_peer",
            legacy_json => <<"vdf_client_peers">>, list => true,
            eager => false},
        #{option => vdf_server, legacy_cli => "vdf_server_trusted_peer",
            legacy_json => <<"vdf_server_trusted_peers">>, list => true,
            eager => false}
    ].

value_str(ip) -> "1.2.3.4:1984";
value_str(ip_no_port) -> "1.2.3.4";
value_str(hostname) -> "myhost:1984";
value_str(hostname_no_port) -> "myhost".

expected(OptionSpec, InputKind) ->
    Value = case {InputKind, maps:get(eager, OptionSpec)} of
        {ip, _} -> {1, 2, 3, 4, 1984};
        {ip_no_port, _} -> {1, 2, 3, 4, 1984};
        {hostname, true} -> {1, 2, 3, 4, 1984};
        {hostname, false} -> <<"myhost:1984">>;
        {hostname_no_port, true} -> {1, 2, 3, 4, 1984};
        {hostname_no_port, false} -> <<"myhost:1984">>
    end,
    case maps:get(list, OptionSpec) of
        true -> [Value];
        false -> Value
    end.

%% Apply a single peer value for an option via one format in an isolated
%% config snapshot and read back the canonical stored value.
store_value(legacy_cli, OptionSpec, Value) ->
    arweave_config:with_test_config(fun() ->
        ok = arweave_config_format_legacy_cli:parse(
            [maps:get(legacy_cli, OptionSpec), Value]),
        read_option(OptionSpec)
    end);
store_value(cli, OptionSpec, Value) ->
    arweave_config:with_test_config(fun() ->
        Arg = "--peers." ++ atom_to_list(maps:get(option, OptionSpec)),
        {ok, Map} = arweave_config_format_cli:parse([Arg, Value]),
        ok = arweave_config:load(Map),
        read_option(OptionSpec)
    end);
store_value(legacy_json, OptionSpec, Value) ->
    arweave_config:with_test_config(fun() ->
        {ok, _} = arweave_config_format_legacy_json:parse(
            legacy_json_config(OptionSpec, Value)),
        read_option(OptionSpec)
    end);
store_value(json, OptionSpec, Value) ->
    arweave_config:with_test_config(fun() ->
        {ok, Map} = arweave_config_format_json:parse(
            json_config(OptionSpec, Value)),
        ok = arweave_config:load(Map),
        read_option(OptionSpec)
    end);
store_value(yaml, OptionSpec, Value) ->
    arweave_config:with_test_config(fun() ->
        {ok, Map} = arweave_config_format_yaml:parse(
            yaml_config(OptionSpec, Value)),
        ok = arweave_config:load(Map),
        read_option(OptionSpec)
    end).

read_option(OptionSpec) ->
    arweave_config:get([peers, maps:get(option, OptionSpec)]).

%% The cli/json/yaml formats key peers by their canonical option path,
%% `peers.<option>'.
config_map(OptionSpec, Value) ->
    OptionBin = atom_to_binary(maps:get(option, OptionSpec), utf8),
    #{<<"peers">> => #{OptionBin => config_value(OptionSpec, Value)}}.

json_config(OptionSpec, Value) ->
    iolist_to_binary(jiffy:encode(config_map(OptionSpec, Value))).

yaml_config(OptionSpec, Value) ->
    {ok, Yaml} = arweave_config_format_yaml:encode(config_map(OptionSpec, Value)),
    iolist_to_binary(Yaml).

legacy_json_config(OptionSpec, Value) ->
    Key = maps:get(legacy_json, OptionSpec),
    iolist_to_binary(jiffy:encode(#{Key => config_value(OptionSpec, Value)})).

config_value(OptionSpec, Value) ->
    Bin = list_to_binary(Value),
    case maps:get(list, OptionSpec) of
        true -> [Bin];
        false -> Bin
    end.

mock_safe_parse_peer(Peer) ->
    Bin = iolist_to_binary([Peer]),
    case Bin of
        <<"myhost">> -> {ok, [{1, 2, 3, 4, 1984}]};
        <<"myhost:", PortBin/binary>> ->
            {ok, [{1, 2, 3, 4, binary_to_integer(PortBin)}]};
        _ -> meck:passthrough([Peer])
    end.
