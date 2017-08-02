-module(app_stocktips).
-export([start/1, new_transaction/2]).
-include("ar.hrl").

%%% An app that monitors a weave for mentions of a company stock ticker,
%%% then attempts to calculate whether the mention was positive or negative.

%% Start a new stocktip server.
start(Stock) -> adt_simple:start(?MODULE, Stock).

%% Scan every new transaction. Takes the app state (the stock to look for)
%% and the transaction.
new_transaction(Stock, TX) ->
	case count_instances(Stock, TX#tx.data) of
		0 -> ok;
		_ ->
			LowerStr = string:to_lower(bitstring_to_list(TX#tx.data)),
			report(Stock,
				count_instances("buy", LowerStr),
				count_instances("sell", LowerStr)
			)
	end,
	Stock.

%% Count the number of times a substring occus in a string.
count_instances(Target, Str) ->
	case re:run(Str, Target, [global]) of
		nomatch -> 0;
		{match, Matches} -> length(Matches)
	end.

%% Report the new stock tip information to the user
report(Stock, Buy, Sell) ->
	io:format("STOCK REPORT: ~p. Buy: ~p. Sell: ~p.~n", [Stock, Buy, Sell]).
