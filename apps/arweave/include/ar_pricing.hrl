%% @doc Pricing macros.

%% For a new account, we charge the fee equal to the price of uploading
%% this number of bytes. The fee is about 0.1$ at the time.
-define(NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT, 20_000_000).

%% The target number of replications.
-ifdef(DEBUG).
-define(N_REPLICATIONS, fun(_MACRO_Height) -> 200 end).
-else.
-define(N_REPLICATIONS, fun(MACRO_Height) ->
	MACRO_Forks = {
		ar_fork:height_2_5(),
		ar_fork:height_2_6()
	},
	case MACRO_Forks of
		{_MACRO_Fork_2_5, MACRO_Fork_2_6} when MACRO_Height >= MACRO_Fork_2_6 ->
			20;
		{MACRO_Fork_2_5, _MACRO_Fork_2_6} when MACRO_Height >= MACRO_Fork_2_5 ->
			45;
		_ ->
			10
	end
end).
-endif.

%% The miners always receive ?MINER_FEE_SHARE of the transaction fees, even
%% when the fees are bigger than the required minimum.
-define(MINER_FEE_SHARE, {1, 21}).

%% When a double-signing proof is provided, we reward the prover with the
%% ?DOUBLE_SIGNING_PROVER_REWARD_SHARE of the minimum reward among the preceding
%% ?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE blocks.
-ifdef(DEBUG).
-define(DOUBLE_SIGNING_REWARD_SAMPLE_SIZE, 2).
-else.
-define(DOUBLE_SIGNING_REWARD_SAMPLE_SIZE, 100).
-endif.

%% When a double-signing proof is provided, we reward the prover with the
%% ?DOUBLE_SIGNING_PROVER_REWARD_SHARE of the minimum reward among the preceding
%% ?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE blocks.
-define(DOUBLE_SIGNING_PROVER_REWARD_SHARE, {1, 2}).

%% Every transaction fee has to be at least
%% X + X * ?MINER_MINIMUM_ENDOWMENT_CONTRIBUTION_SHARE
%% where X is the amount sent to the endowment pool.
-define(MINER_MINIMUM_ENDOWMENT_CONTRIBUTION_SHARE, {1, 20}).

%% The fixed USD to AR rate used after the fork 2.6 until the automatic transition to the new
%% pricing scheme is complete. We fix the rate because the network difficulty is expected
%% fluctuate a lot around the fork.
-define(FORK_2_6_PRE_TRANSITION_USD_TO_AR_RATE, {1, 10}).

%% The number of blocks which have to pass since the 2.6.8 fork before we
%% start mixing in the new fee calculation method.
-ifdef(DEBUG).
	-define(PRICE_2_6_8_TRANSITION_START, 2).
-else.
	-ifndef(PRICE_2_6_8_TRANSITION_START).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_6_8_TRANSITION_START, 0).
		-else.
			-define(PRICE_2_6_8_TRANSITION_START, (30 * 24 * 190)). % ~6.5 months / Dec. 14, 2023
		-endif.
	-endif.
-endif.

%% The number of blocks following the 2.6.8 + ?PRICE_2_6_8_TRANSITION_START block
%% where the tx fee computation is transitioned to the new calculation method.
%% Let TransitionStart = fork 2.6.8 height + ?PRICE_2_6_8_TRANSITION_START.
%% Let A = height - TransitionStart + 1.
%% Let B = TransitionStart + ?PRICE_2_6_8_TRANSITION_BLOCKS - (height + 1).
%% Then price per GiB-minute = price old * B / (A + B) + price new * A / (A + B).
-ifdef(DEBUG).
	-define(PRICE_2_6_8_TRANSITION_BLOCKS, 2).
-else.
	-ifndef(PRICE_2_6_8_TRANSITION_BLOCKS).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_6_8_TRANSITION_BLOCKS, 0).
		-else.
			-ifndef(PRICE_2_6_8_TRANSITION_BLOCKS).
				-define(PRICE_2_6_8_TRANSITION_BLOCKS, (30 * 24 * 30 * 18)). % ~18 months.
			-endif.
		-endif.
	-endif.
-endif.

-ifdef(DEBUG).
	-define(PRICE_PER_GIB_MINUTE_PRE_TRANSITION, 8162).
-else.
	%% STATIC_2_6_8_FEE_WINSTON / (200 (years) * 365 (days) * 24 * 60) / 20 (replicas)
	%% = ~400 Winston per GiB per minute.
	-define(PRICE_PER_GIB_MINUTE_PRE_TRANSITION, 400).
-endif.

%% The number of recent blocks contributing data points to the continuous estimation
%% of the average price of storing a gibibyte for a minute. Also, the reward history
%% is used to tracking the reserved mining rewards.
-ifdef(DEBUG).
	-define(REWARD_HISTORY_BLOCKS, 3).
-else.
	-ifndef(REWARD_HISTORY_BLOCKS).
		-define(REWARD_HISTORY_BLOCKS, (30 * 24 * 30)).
	-endif.
-endif.

-ifdef(DEBUG).
	-define(BLOCK_TIME_HISTORY_BLOCKS, 3).
-else.
	-ifndef(BLOCK_TIME_HISTORY_BLOCKS).
		-define(BLOCK_TIME_HISTORY_BLOCKS, (30 * 24 * 30)).
	-endif.
-endif.

%% The prices are re-estimated every so many blocks.
-ifdef(DEBUG).
-define(PRICE_ADJUSTMENT_FREQUENCY, 2).
-else.
	-ifndef(PRICE_ADJUSTMENT_FREQUENCY).
		-define(PRICE_ADJUSTMENT_FREQUENCY, 50).
	-endif.
-endif.

%% An approximation of the natural logarithm of ?PRICE_DECAY_ANNUAL (0.995),
%% expressed as a decimal fraction, with the precision of math:log.
-define(LN_PRICE_DECAY_ANNUAL, {-5012541823544286, 1000000000000000000}).

%% The assumed annual decay rate of the Arweave prices, expressed as a decimal fraction.
-define(PRICE_DECAY_ANNUAL, {995, 1000}). % 0.995, i.e., 0.5% annual decay rate.

%% The precision of computing the natural exponent as a decimal fraction,
%% expressed as the maximal power of the argument in the Taylor series.
-define(TX_PRICE_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION, 9).

%% When/if the endowment fund runs empty, we increase storage fees and lock a "Kryder+ rate
%% multiplier latch" to make sure we do not increase the fees several times while the
%% endowment size remains low. Once the endowment is bigger than this constant again,
%% we open the latch (and will increase the fees again when/if the endowment is empty).
%% The value is redenominated according the denomination used at the time.
-ifdef(DEBUG).
-define(RESET_KRYDER_PLUS_LATCH_THRESHOLD, 100_000_000_000).
-else.
-define(RESET_KRYDER_PLUS_LATCH_THRESHOLD, 10_000_000_000_000_000).
-endif.

%% The total supply, in Winston (the sum of genesis balances + the total emission).
%% Does NOT include the additional emission which may start in the far future if and when
%% the endowment pool runs empty.
-ifdef(DEBUG).
	%% The debug constant is not always actually equal to the sum of genesis balances plust
	%% the total emission. We just set a relatively low value so that we can reproduce
	%% autoredenomination in tests.
	-define(TOTAL_SUPPLY, 1500000000000).
-else.
	-ifdef(FORKS_RESET).
		%% This value should be ideally adjusted if the genesis balances
		%% of a new weave differ from those in mainnet.
		-define(TOTAL_SUPPLY, 66000015859279336957).
	-else.
		-define(TOTAL_SUPPLY, 66_000_015_859_279_336_957).
	-endif.
-endif.

%% Re-denominate AR (multiply by 1000) when the available supply falls below this
%% number of units.
-ifdef(DEBUG).
-define(REDENOMINATION_THRESHOLD, 1350000000000).
-else.
-define(REDENOMINATION_THRESHOLD, 1000_000_000_000_000_000).
-endif.

%% The number of blocks which has to pass after we assign the redenomination height before
%% the redenomination occurs. Transactions without an explicitly assigned denomination are
%% not allowed in these blocks. The motivation is to protect the legacy libraries' users from
%% an attack where a post-redenomination transaction is included in a pre-redenomination
%% block, potentially charging the user a thousand times the intended fee or transfer amount.
-ifdef(DEBUG).
-define(REDENOMINATION_DELAY_BLOCKS, 2).
-else.
-define(REDENOMINATION_DELAY_BLOCKS, 100).
-endif.

%% USD to AR exchange rates by height defined together with INITIAL_USD_TO_AR_HEIGHT
%% and INITIAL_USD_TO_AR_DIFF. The protocol uses these constants to estimate the
%% USD to AR rate at any block based on the change in the network difficulty and inflation
%% rewards.
-define(INITIAL_USD_TO_AR(Height), fun() ->
	Forks = {
		ar_fork:height_2_4(),
		ar_fork:height_2_5()
	},
	case Forks of
		{_Fork_2_4, Fork_2_5} when Height >= Fork_2_5 ->
			{1, 65};
		{Fork_2_4, _Fork_2_5} when Height >= Fork_2_4 ->
			?INITIAL_USD_TO_AR_PRE_FORK_2_5
	end
end).

%% The original USD to AR conversion rate, defined as a fraction. Set up at fork 2.4.
%% Used until the fork 2.5.
-define(INITIAL_USD_TO_AR_PRE_FORK_2_5, {1, 5}).

%% The network difficulty at the time when the USD to AR exchange rate was
%% ?INITIAL_USD_TO_AR(Height). Used to account for the change in the network
%% difficulty when estimating the new USD to AR rate.
-define(INITIAL_USD_TO_AR_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2(),
		ar_fork:height_2_5()
	},
	case Forks of
		{_Fork_1_9, _Fork_2_2, Fork_2_5} when Height >= Fork_2_5 ->
			32;
		{_Fork_1_9, Fork_2_2, _Fork_2_5} when Height >= Fork_2_2 ->
			34;
		{Fork_1_9, _Fork_2_2, _Fork_2_5} when Height < Fork_1_9 ->
			28;
		_ ->
			29
	end
end).

%% The network height at the time when the USD to AR exchange rate was
%% ?INITIAL_USD_TO_AR(Height). Used to account for the change in inflation
%% rewards when estimating the new USD to AR rate.
-define(INITIAL_USD_TO_AR_HEIGHT(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2(),
		ar_fork:height_2_5(),
		ar_fork:height_2_6()
	},
	%% In case the fork heights are reset to 0 (e.g. on testnets),
	%% set the initial height to 1 - the height where the inflation
	%% emission essentially begins.
	case Forks of
		{_Fork_1_9, _Fork_2_2, _Fork_2_5, Fork_2_6} when Height >= Fork_2_6 ->
			max(Fork_2_6, 1);
		{_Fork_1_9, _Fork_2_2, Fork_2_5, _Fork_2_6} when Height >= Fork_2_5 ->
			max(Fork_2_5, 1);
		{_Fork_1_9, Fork_2_2, _Fork_2_5, _Fork_2_6} when Height >= Fork_2_2 ->
			max(Fork_2_2, 1);
		{Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6} when Height < Fork_1_9 ->
			max(ar_fork:height_1_8(), 1);
		{Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6} ->
			max(Fork_1_9, 1)
	end
end).

%% The base wallet generation fee in USD, defined as a fraction.
%% The amount in AR depends on the current difficulty and height.
%% Used until the transition to the new fee calculation method is complete.
-define(WALLET_GEN_FEE_USD, {1, 10}).

%% The estimated historical price of storing 1 GB of data for the year 2018,
%% expressed as a decimal fraction.
%% Used until the transition to the new fee calculation method is complete.
-define(USD_PER_GBY_2018, {1045, 1000000}). % 0.001045

%% The estimated historical price of storing 1 GB of data for the year 2019,
%% expressed as a decimal fraction.
%% Used until the transition to the new fee calculation method is complete.
-define(USD_PER_GBY_2019, {925, 1000000}). % 0.000925

-define(STATIC_2_6_8_FEE_WINSTON, 858_000_000_000).

%% The largest possible multiplier for a one-step increase of the USD to AR Rate.
-define(USD_TO_AR_MAX_ADJUSTMENT_UP_MULTIPLIER, {1005, 1000}).

%% The largest possible multiplier for a one-step decrease of the USD to AR Rate.
-define(USD_TO_AR_MAX_ADJUSTMENT_DOWN_MULTIPLIER, {995, 1000}).

%% Reduce the USD to AR fraction if both the dividend and the devisor become bigger than this.
-ifdef(DEBUG).
-define(USD_TO_AR_FRACTION_REDUCTION_LIMIT, 100).
-else.
-define(USD_TO_AR_FRACTION_REDUCTION_LIMIT, 1000000).
-endif.

%% Every transaction fee has to be at least X + X * ?MINING_REWARD_MULTIPLIER
%% where X is the amount sent to the endowment pool.
%% Used until the transition to the new fee calculation method is complete.
-ifdef(DEBUG).
-define(MINING_REWARD_MULTIPLIER, {2, 10000}).
-else.
-define(MINING_REWARD_MULTIPLIER, {2, 10}).
-endif.

%% The USD to AR exchange rate for a new chain, e.g. a testnet.
-define(NEW_WEAVE_USD_TO_AR_RATE, ?INITIAL_USD_TO_AR_PRE_FORK_2_5).

%% Initial $/AR exchange rate. Used until the fork 2.4.
-define(INITIAL_USD_PER_AR(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2()
	},
	case Forks of
		{Fork_1_9, _Fork_2_2} when Height < Fork_1_9 ->
			1.5;
		{_Fork_1_9, Fork_2_2} when Height >= Fork_2_2 ->
			4;
		_ ->
			1.2
	end
end).

%% Base wallet generation fee. Used until fork 2.2.
-define(WALLET_GEN_FEE, 250000000000).
