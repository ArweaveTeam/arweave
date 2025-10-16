-module(ar_rewards).

-export([reward_history_length/1, expected_hashes_length/1, buffered_reward_history_length/1, 
		set_reward_history/2, get_locked_rewards/1,
		trim_locked_rewards/2, trim_reward_history/2,
		trim_buffered_reward_history/2, get_oldest_locked_address/1,
		add_element/2, has_locked_reward/2,
		reward_history_hash/3, validate_reward_history_hashes/3,
		get_total_reward_for_address/2, get_reward_history_totals/1,
		apply_rewards/2, apply_reward/4, log_reward_history/3]).

-include_lib("arweave/include/ar.hrl").

reward_history_length(Height) ->
	min(
		Height - ar_fork:height_2_6() + 1, %% included for compatibility with unit tests
		case Height >= ar_fork:height_2_8() of
			true ->
				ar_testnet:reward_history_blocks(Height) + ar_block:get_consensus_window_size();
			false ->
				ar_testnet:legacy_reward_history_blocks(Height) + ar_block:get_consensus_window_size()
		end
	).

expected_hashes_length(Height) ->
	case Height >= ar_fork:height_2_8() of
		true ->
			%% Take one more block.reward_history_hash because after 2.8 we use
			%% the previous reward history hash to compute the new one.
			ar_block:get_consensus_window_size() + 1;
		false ->
			ar_block:get_consensus_window_size()
		end.

%% @doc The reward history that gets cached in #block and returned by /reward_history has
%% to be long enough to include:
%% 1. The current reward history (i.e. reward_history_length(Height))
%% 2. The reward history that was in use recently
%%    (i.e. reward_history_length(Height - expected_hashes_length(Height)))
%% 3. The current locked rewards (i.e. locked_rewards_blocks(Height))
buffered_reward_history_length(Height) ->
	max(
		max(
			reward_history_length(Height - expected_hashes_length(Height)),
			reward_history_length(Height)
		),
		ar_testnet:locked_rewards_blocks(Height)
	).

%% @doc Add the corresponding reward history to every block record. We keep
%% the reward histories in the block cache and use them to validate blocks applied on top.
%%
%% The expectation is that RewardHistory is at least
%% reward_history_length/1 long, and that Blocks is no longer than
%% ar_block:get_consensus_window_size(). If so then each block.reward_history value will be at least
%% ?REWARD_HISTORY_BLOCKS long.
set_reward_history([], _RewardHistory) ->
	[];
set_reward_history(Blocks, []) ->
	Blocks;
set_reward_history([B | Blocks], RewardHistory) ->
	[B#block{ reward_history = RewardHistory } | set_reward_history(Blocks, tl(RewardHistory))].

%% @doc Return the most recent part of the reward history including the locked rewards.
get_locked_rewards(B) ->
	trim_locked_rewards(B#block.height, B#block.reward_history).

%% @doc Trim RewardHistory to just the locked rewards.
trim_locked_rewards(Height, RewardHistory) ->
	LockRewardsLength = ar_testnet:locked_rewards_blocks(Height),
	lists:sublist(RewardHistory, LockRewardsLength).

%% @doc Trim RewardHistory to the values that will be stored in the block. This is the
%% sliding window plus a buffer of ar_block:get_consensus_window_size() values.
trim_reward_history(Height, RewardHistory) ->
	lists:sublist(RewardHistory, reward_history_length(Height)).

%% @doc See the buffered_reward_history_length/1 function for the distinction between
%% reward_history_length/1 and buffered_reward_history_length/1.
trim_buffered_reward_history(Height, RewardHistory) ->
	lists:sublist(RewardHistory, buffered_reward_history_length(Height)).

get_oldest_locked_address(B) ->
	LockedRewards = get_locked_rewards(B),
	{Addr, _HashRate, _Reward, _Denomination} = lists:last(LockedRewards),
	Addr.

%% @doc Add a new {Addr, HashRate, Reward, Denomination} tuple to the reward history.
add_element(B, RewardHistory) ->
	Height = B#block.height,
	Reward = B#block.reward,
	HashRate = ar_difficulty:get_hash_rate_fixed_ratio(B),
	Denomination = B#block.denomination,
	RewardAddr = B#block.reward_addr,
	trim_buffered_reward_history(Height, 
		[{RewardAddr, HashRate, Reward, Denomination} | RewardHistory]).

has_locked_reward(_Addr, []) ->
	false;
has_locked_reward(Addr, [{Addr, _, _, _} | _]) ->
	true;
has_locked_reward(Addr, [_ | RewardHistory]) ->
	has_locked_reward(Addr, RewardHistory).

validate_reward_history_hashes(_Height, _RewardHistory, []) ->
	true;
validate_reward_history_hashes(0, [_Element] = History, [H]) ->
	%% This clause is not applicable in mainnet but reflects how we initialize
	%% the reward history hash in the new weaves, even if the 2.8 height is not
	%% set from the genesis.
	H == reward_history_hash(0, <<>>, History);
validate_reward_history_hashes(Height, RewardHistory, [H, PrevH | ExpectedHashes]) ->
	case validate_reward_history_hash(Height, PrevH, H, RewardHistory) of
		true ->
			case ExpectedHashes of
				[] ->
					true;
				_ ->
					validate_reward_history_hashes(Height - 1,
							tl(RewardHistory), [PrevH | ExpectedHashes])
			end;
		false ->
			false
	end;
validate_reward_history_hashes(Height, RewardHistory, [H]) ->
	%% After 2.8 we always include one extra hash to the list so we cannot end up here.
	true = Height < ar_fork:height_2_8(),
	validate_reward_history_hash(Height, not_set, H, RewardHistory).

validate_reward_history_hash(Height, PreviousRewardHistoryHash, H, RewardHistory) ->
	H == reward_history_hash(Height, PreviousRewardHistoryHash,
			%% Pre-2.8: slice the reward history to compute the hash
			%% Post-2.8: use the previous reward history hash and the head of the history to compute
			%% the new hash.
			trim_locked_rewards(Height, RewardHistory)).

reward_history_hash(Height, PreviousRewardHistoryHash, History) ->
	case Height >= ar_fork:height_2_8() of
		true ->
			Element = encode_reward_history_element(hd(History)),
			Preimage = << Element/binary, PreviousRewardHistoryHash/binary >>,
			crypto:hash(sha256, Preimage);
		false ->
			reward_history_hash(History, [ar_serialize:encode_int(length(History), 8)])
	end.

encode_reward_history_element({Addr, HashRate, Reward, Denomination}) ->
	HashRateBin = ar_serialize:encode_int(HashRate, 8),
	RewardBin = ar_serialize:encode_int(Reward, 8),
	DenominationBin = << Denomination:24 >>,
	crypto:hash(sha256, << Addr/binary, HashRateBin/binary,
			RewardBin/binary, DenominationBin/binary >>).

reward_history_hash([], IOList) ->
	crypto:hash(sha256, iolist_to_binary(IOList));
reward_history_hash([{Addr, HashRate, Reward, Denomination} | History], IOList) ->
	HashRateBin = ar_serialize:encode_int(HashRate, 8),
	RewardBin = ar_serialize:encode_int(Reward, 8),
	DenominationBin = << Denomination:24 >>,
	reward_history_hash(History, [Addr, HashRateBin, RewardBin, DenominationBin | IOList]).

get_total_reward_for_address(Addr, B) ->
	get_total_reward_for_address(Addr, get_locked_rewards(B), B#block.denomination, 0).

get_total_reward_for_address(_Addr, [], _Denomination, Total) ->
	Total;
get_total_reward_for_address(Addr, [{Addr, _, Reward, RewardDenomination} | LockedRewards], Denomination, Total) ->
	Reward2 = ar_pricing:redenominate(Reward, RewardDenomination, Denomination),
	get_total_reward_for_address(Addr, LockedRewards, Denomination, Total + Reward2);
get_total_reward_for_address(Addr, [_ | LockedRewards], Denomination, Total) ->
	get_total_reward_for_address(Addr, LockedRewards, Denomination, Total).

%% @doc Return {HashRateTotal, RewardTotal} summed up over the entire
%% sliding window of the history of rewards for the given block.
get_reward_history_totals(B) ->
	Denomination = B#block.denomination,
	History = trim_reward_history(B#block.height, B#block.reward_history),
	log_reward_history("get_reward_history_totals", History, 200),
	{HashRateTotal, RewardTotal} = get_totals(History, Denomination, 0, 0),
	{HashRateTotal, RewardTotal, History}.

get_totals([], _Denomination, HashRateTotal, RewardTotal) ->
	{HashRateTotal, RewardTotal};
get_totals([{_Addr, HashRate, Reward, RewardDenomination} | History],
		Denomination, HashRateTotal, RewardTotal) ->
	HashRateTotal2 = HashRateTotal + HashRate,
	Reward2 = ar_pricing:redenominate(Reward, RewardDenomination, Denomination),
	RewardTotal2 = RewardTotal + Reward2,
	get_totals(History, Denomination, HashRateTotal2, RewardTotal2).

apply_rewards(PrevB, Accounts) ->
	%% The only time we won't have only a single reward to apply is if the
	%% ?LOCKED_REWARDS_BLOCKS has changed between blocks. And currently that can only
	%% happen on testnet.
	Height = PrevB#block.height,
	NumRewardsToApply = max(0,
			ar_testnet:locked_rewards_blocks(Height) -
			ar_testnet:locked_rewards_blocks(Height + 1) + 1),
	true = NumRewardsToApply == 1 orelse ar_testnet:is_testnet(),

	%% Get the last NumRewardsToApply elements of the LockedRewards list in reverse order.
	%% Normally this will be a list with a single element: the last element in the
	%% LockedRewards list.
	%% When forking testnet off of mainnet this may be a list of more than 1 element.
	RewardsToApply = lists:sublist(lists:reverse(get_locked_rewards(PrevB)), NumRewardsToApply), 

	apply_rewards2(RewardsToApply, PrevB#block.denomination, Accounts).

apply_rewards2([], _Denomination, Accounts) ->
	Accounts;
apply_rewards2([{Addr, _HashRate, Reward, RewardDenomination} | RewardsToApply],
		Denomination, Accounts) ->
	case ar_node_utils:is_account_banned(Addr, Accounts) of
		true ->
			apply_rewards2(RewardsToApply, Denomination, Accounts);
		false ->
			Reward2 = ar_pricing:redenominate(Reward, RewardDenomination,
					Denomination),
			Accounts2 = apply_reward(Accounts, Addr, Reward2, Denomination),
			apply_rewards2(RewardsToApply, Denomination, Accounts2)
	end.

%% @doc Add the mining reward to the corresponding account.
apply_reward(Accounts, unclaimed, _Quantity, _Denomination) ->
	Accounts;
apply_reward(Accounts, RewardAddr, Amount, Denomination) ->
	case maps:get(RewardAddr, Accounts, not_found) of
		not_found ->
			ar_node_utils:update_account(RewardAddr, Amount, <<>>, Denomination, true, Accounts);
		{Balance, LastTX} ->
			Balance2 = ar_pricing:redenominate(Balance, 1, Denomination),
			ar_node_utils:update_account(RewardAddr, Balance2 + Amount, LastTX,
				Denomination, true, Accounts);
		{Balance, LastTX, AccountDenomination, MiningPermission} ->
			Balance2 = ar_pricing:redenominate(Balance, AccountDenomination, Denomination),
			ar_node_utils:update_account(RewardAddr, Balance2 + Amount, LastTX,
				Denomination, MiningPermission, Accounts)
	end.

log_reward_history(Message, RewardHistory, N) ->
	Length = length(RewardHistory),
	LimitedRewardHistory = lists:sublist(RewardHistory, N),
	LogEntries = lists:map(fun({Addr, HashRate, Reward, Denomination}) ->
		EncodedAddr = ar_util:encode(Addr),
		LogHashRate = math:log10(HashRate),
		io_lib:format("{~s, ~p, ~p, ~p}", 
						[EncodedAddr, LogHashRate, Reward, Denomination])
	end, LimitedRewardHistory),
	LogString = string:join(LogEntries, "; "),
	?LOG_INFO("~s Length: ~p, Entries: ~s", [Message, Length, LogString]).	