-module(ar_start_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%% @doc A configuration that breaks a rule spanning several options stops the
%% start before the node's supervisor starts.
invalid_config_stops_before_start_test_() ->
    ar_test_util:with_mocked([
        {ar_logger, init, fun() -> ok end},
        {ar_sup, start_link, fun() -> erlang:error(node_started) end}
    ], fun() ->
        arweave_config:internal_with_test_config(fun() ->
            ok = arweave_config:internal_force_config(#{
                [verify, mode] => purge,
                [mining, enabled] => true
            }),
            ?assertMatch({error, _}, ar:start(normal, [])),
            ?assertNot(meck:called(ar_sup, start_link, '_'))
        end)
    end).
