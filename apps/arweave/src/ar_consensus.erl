-module(ar_consensus).

-export([is_testnet/0, testnet_fork_height/0, test_wallet_top_up/1,
        locked_rewards_blocks/1, reward_history_blocks/1, target_block_time/1,
        legacy_reward_history_blocks/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

%%% The consensus parameters that differ between mainnet and a testnet: a
%%% testnet build reads them from the `testnet` option group from its fork
%%% height on, every other build keeps the mainnet values.

-ifdef(TESTNET).

is_testnet() ->
    true.

%% @doc The height of the first testnet block.
testnet_fork_height() ->
    arweave_config:get([testnet, fork_height]).

%% @doc The {Address, Winston} credit the test wallet receives at the given
%% height, not_set at every other height.
test_wallet_top_up(Height) ->
    case Height == testnet_fork_height() of
        true ->
            case arweave_config:get([testnet, test_wallet_address]) of
                not_set ->
                    not_set;
                Addr ->
                    TopUp = arweave_config:get([testnet, test_wallet_top_up]),
                    {Addr, ?AR(TopUp)}
            end;
        false ->
            not_set
    end.

locked_rewards_blocks(Height) ->
    case Height >= testnet_fork_height() of
        true -> arweave_config:get([testnet, locked_rewards_blocks]);
        false -> ?LOCKED_REWARDS_BLOCKS
    end.

reward_history_blocks(Height) ->
    case Height >= testnet_fork_height() of
        true -> arweave_config:get([testnet, reward_history_blocks]);
        false -> ?REWARD_HISTORY_BLOCKS
    end.

legacy_reward_history_blocks(Height) ->
    case Height >= testnet_fork_height() of
        true -> arweave_config:get([testnet, legacy_reward_history_blocks]);
        false -> ?LEGACY_REWARD_HISTORY_BLOCKS
    end.

target_block_time(Height) ->
    case Height >= testnet_fork_height() of
        true -> arweave_config:get([testnet, target_block_time]);
        false -> ?TARGET_BLOCK_TIME
    end.

-else.

is_testnet() ->
    false.

%% @doc No block is a testnet block outside testnet builds.
testnet_fork_height() ->
    infinity.

test_wallet_top_up(_Height) ->
    not_set.

locked_rewards_blocks(_Height) ->
    ?LOCKED_REWARDS_BLOCKS.

reward_history_blocks(_Height) ->
    ?REWARD_HISTORY_BLOCKS.

legacy_reward_history_blocks(_Height) ->
    ?LEGACY_REWARD_HISTORY_BLOCKS.

target_block_time(_Height) ->
    ?TARGET_BLOCK_TIME.

-endif.
