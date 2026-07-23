%%% @doc Specs for the `randomx` option group. Options for
%%% selecting RandomX execution characteristics.
-module(arweave_config_options_randomx).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [randomx, jit],
            default => true,
            type => boolean,
            legacy => randomx_jit,
            short_description =>
                <<"Enable RandomX JIT compilation.">>
        },
        #{
            enabled => true,
            option_key => [randomx, hardware_aes],
            runtime => true,
            default => true,
            type => boolean,
            legacy => randomx_hardware_aes,
            short_description =>
                <<"Enable RandomX hardware AES acceleration.">>
        },
        #{
            enabled => true,
            option_key => [randomx, large_pages],
            default => false,
            type => boolean,
            legacy => randomx_large_pages,
            short_description =>
                <<"Use large memory pages for RandomX.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Tune RandomX execution behavior.">>.
