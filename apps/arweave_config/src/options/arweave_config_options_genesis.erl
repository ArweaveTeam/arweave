%%% @doc Specs for the `genesis` option group. Options for
%%% controlling bootstrap of a new local weave.
-module(arweave_config_options_genesis).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [genesis, init],
            default => false,
            type => boolean,
            legacy => init,
            short_description =>
                <<"Start a new weave (genesis bootstrap).">>,
            long_description =>
                <<"Rejected if the configured network name is "
                  "mainnet.">>
        },
        #{
            enabled => true,
            option_key => [genesis, difficulty],
            default => ?DEFAULT_DIFF,
            type => pos_integer,
            legacy => diff,
            short_description =>
                <<"Initial mining difficulty (genesis bootstrap "
                  "only).">>
        }
    ].

validate() ->
    case arweave_config:get([genesis, init]) of
        true ->
            case ?NETWORK_NAME of
                "arweave.N.1" ->
                    {error, <<"Cannot start a new network with the mainnet name! "
                            "Use ./bin/start-localnet ... when running from sources "
                            "or compile via ./rebar3 as localnet tar and use "
                            "./bin/start ... as usual.">>};
                _ ->
                    ok
            end;
        _ ->
            ok
    end.

group_description() ->
    <<"Control new-weave bootstrap behavior.">>.
