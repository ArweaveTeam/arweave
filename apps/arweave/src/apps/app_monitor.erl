-module(app_monitor).
-export([start/0, new_block/1]).
-include("ar.hrl").

%%% An Arweave monitoring app. Scans new (confirmed) transactions for archived
%%% web pages that match a given regular expression.

%% The regular expression to match on.
-define(REGEX, "My Company Name").

%% Start the server as an adt_simple app, calling back to this module.
start() ->
	adt_simple:start(?MODULE).

%% This function is called and passed a new block every time one is added
%% to the blockweave.
new_block(B) ->
	io:format("New block: ~p. Number of transactions: ~p.~n",
		[B#block.height, length(B#block.txs)]).

%% Report that a new transaction meets our criteria.
report(T) ->
	% Log the transaction information to the console.
	io:format("TX ~p matches selection criteria!~n", [T#tx.id]).
