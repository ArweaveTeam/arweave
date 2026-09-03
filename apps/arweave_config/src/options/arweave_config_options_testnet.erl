%%% @doc Specs for the `testnet` option group: the consensus parameters a
%%% testnet build (`rebar3 as testnet`) applies from its fork height on.
%%% Other builds reject any of them being set.
-module(arweave_config_options_testnet).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [testnet, fork_height],
            default => not_set,
            type => pos_integer,
            short_description =>
                <<"Height of the first testnet block (testnet builds "
                  "only).">>,
            long_description =>
                <<"One above the tip of the state the testnet starts "
                  "from, and a multiple of 10 so that it is a difficulty "
                  "retarget height. At this height the difficulty drops "
                  "and the other testnet parameters apply. Required by "
                  "testnet builds.">>
        },
        #{
            enabled => true,
            option_key => [testnet, target_block_time],
            default => ?TARGET_BLOCK_TIME,
            type => pos_integer,
            short_description =>
                <<"Target block time in seconds from the fork height on.">>
        },
        #{
            enabled => true,
            option_key => [testnet, test_wallet_address],
            default => not_set,
            type => address,
            short_description =>
                <<"Wallet credited with test_wallet_top_up AR at the fork "
                  "height.">>
        },
        #{
            enabled => true,
            option_key => [testnet, test_wallet_top_up],
            default => 1_000_000,
            type => pos_integer,
            short_description =>
                <<"AR credited to test_wallet_address at the fork "
                  "height.">>
        },
        #{
            enabled => true,
            option_key => [testnet, reward_history_blocks],
            default => ?REWARD_HISTORY_BLOCKS,
            type => pos_integer,
            short_description =>
                <<"Reward history window in blocks from the fork height "
                  "on.">>
        },
        #{
            enabled => true,
            option_key => [testnet, legacy_reward_history_blocks],
            default => ?LEGACY_REWARD_HISTORY_BLOCKS,
            type => pos_integer,
            short_description =>
                <<"Legacy reward history window in blocks from the fork "
                  "height on.">>
        },
        #{
            enabled => true,
            option_key => [testnet, locked_rewards_blocks],
            default => ?LOCKED_REWARDS_BLOCKS,
            type => pos_integer,
            short_description =>
                <<"Blocks a mining reward stays locked from the fork "
                  "height on.">>
        }
    ].

group_description() ->
    <<"Testnet build parameters.">>.

%% @doc A testnet build needs a fork height at a retarget height; any other
%% build must not carry testnet settings at all.
validate() ->
    Configured = [Key || #{ option_key := Key, default := Default } <- specs(),
        arweave_config:get(Key) =/= Default],
    validate(is_testnet_build(), Configured).

validate(true, _Configured) ->
    case arweave_config:get([testnet, fork_height]) of
        not_set ->
            {error, <<"testnet.fork_height must be set in a testnet "
                      "build.">>};
        Height when is_integer(Height), Height rem ?RETARGET_BLOCKS =:= 0 ->
            ok;
        _ ->
            {error, <<"testnet.fork_height must be a multiple of 10, a "
                      "difficulty retarget height.">>}
    end;
validate(false, []) ->
    ok;
validate(false, Configured) ->
    Names = [atom_to_binary(lists:last(Key)) || Key <- Configured],
    {error, <<"testnet options are only valid in a testnet build: ",
        (iolist_to_binary(lists:join(<<", ">>, Names)))/binary>>}.

-ifdef(TESTNET).
is_testnet_build() ->
    true.
-else.
is_testnet_build() ->
    false.
-endif.
