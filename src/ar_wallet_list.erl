-module(ar_wallet_list).
-export([
	apply_txs/2, apply_tx/2,
	calculate_tx_gen_fee/2,
	check_address_last_tx/3,
	validate/1
	]).

-include("ar.hrl").

%% @doc Update a wallet list with a set of new transactions.
apply_txs(WalletList, TXs) ->
	lists:sort(
		lists:foldl(
			fun(TX, CurrWalletList) ->
				apply_tx(CurrWalletList, TX)
			end,
			WalletList,
			TXs
		)
	).

%% @doc Apply a transaction to a wallet list, updating it.
%% Critically, filter empty wallets from the list after application.
apply_tx(WalletList, unavailable) ->
	WalletList;
apply_tx(WalletList, TX) ->
	filter_empty_wallets(do_apply_tx(WalletList, TX)).

%% @doc Calculate base tx generation fee for a wallet list.
%% called from ar_tx
calculate_tx_gen_fee(_, undefined) -> 0;
calculate_tx_gen_fee(_, <<>>) -> 0;
calculate_tx_gen_fee(WalletList, Address) ->
	case lists:keymember(Address, 1, WalletList) of
		true  -> 0;
		false -> ?WALLET_GEN_FEE
    end.

%% @doc Check if wallet list contains a {Address, _, LastTX} wallet.
%% if so return true else false.
check_address_last_tx(WalletList, Address, LastTX) ->
	case lists:keyfind(Address, 1, WalletList) of
		{Address, _, LastTX} -> true;
		_ -> false
	end.

%% @doc Ensure that all wallets in the wallet list have a positive balance.
validate([]) ->
	true;
validate([{_, 0, Last} | _]) when byte_size(Last) == 0 ->
	false;
validate([{_, Qty, _} | _]) when Qty < 0 ->
	false;
validate([_ | Rest]) ->
	validate(Rest).

%%%% Private

%% @doc Perform the concrete application of a transaction to
%% a prefiltered wallet list.
do_apply_tx(
		WalletList,
		#tx {
			id = ID,
			owner = From,
			last_tx = Last,
			target = To,
			quantity = Qty,
			reward = Reward
		}) ->
	Addr = ar_wallet:to_address(From),
	case lists:keyfind(Addr, 1, WalletList) of
		{Addr, Balance, Last} ->
			NewWalletList = lists:keyreplace(Addr, 1, WalletList, {Addr, Balance - (Qty + Reward), ID}),
			case lists:keyfind(To, 1, NewWalletList) of
				false ->
					[{To, Qty, <<>>} | NewWalletList];
				{To, OldBalance, LastTX} ->
					lists:keyreplace(To, 1, NewWalletList, {To, OldBalance + Qty, LastTX})
			end;
		_ ->
			WalletList
	end.

%% @doc Remove wallets with zero balance from a wallet list.
filter_empty_wallets([]) ->
	[];
filter_empty_wallets([{_, 0, <<>>} | WalletList]) ->
	filter_empty_wallets(WalletList);
filter_empty_wallets([Wallet | Rest]) ->
	[Wallet | filter_empty_wallets(Rest)].
