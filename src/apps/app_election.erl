-module(app_election).
-export([start/1, new_block/2, message/2]).
-export([description/1, vote/4, results/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% The Arweave OpenElections monitor node.
%%% Listens for votes being cast in elections. Returns results when asked.

%% How should the app identify itself in the 'type' field?
-define(APP_TYPE_ID, "Arweave.OpenElection").

-record(state, {
	name, % A string representation of the name of the election.
	desc_tx = "", % TXID of the transaction laying out the terms of the vote.
	votes = [] % List of vote objects.
}).

%% Represents a vote cast in the given election.
-record(vote, {
	tx, % ID of TX that cast vote.
	address, % Address in the system that cast the vote.
	option % Which option did they chose in the election?
}).

%% PUBLIC INTERFACE

%% @doc Start an election (monitor) given a name and a TXID relating to a
%% plain text TX containing a description of the election question(s).
start(Name) -> start(Name, []).
start(Name, DescTX) -> start(Name, DescTX, []).
start(Name, DescTX, Peers) ->
	adt_simple:start(?MODULE, #state { name = Name, desc_tx = DescTX }, Peers).

%% @doc Get the description transaction from the server.
description(PID) ->
	PID ! {get_description, self()},
	receive
		{description, DescTX} -> DescTX
	end.

%% @doc Get the results from the server..
results(PID) ->
	PID ! {get_results, self()},
	receive
		{results, Res} -> Res
	end.

%% @doc Cast a vote in an election, given an electionID.
vote(Node, Wallet, Name, Option) ->
	TX = ar_tx:new(prepare_vote(Name, Option)),
	SignedTX = ar_tx:sign(TX, Wallet),
	ar_node:add_tx(Node, SignedTX),
	ok.

%% @doc Create a new JSON vote representation.
prepare_vote(Name, Option) ->
	list_to_binary(
		ar_serialize:jsonify(
			{struct,
				[
					{"type", ?APP_TYPE_ID},
					{"name", Name},
					{"option", Option}
				]
			}
		)
	).

%%% SERVER FUNCTIONS

%% @doc Take a newly mined block and process the transactions/votes inside.
new_block(S, B) ->
	lists:foldl(fun(TX, NewS) -> new_transaction(NewS, TX) end, S, B#block.txs).

%% @doc Process a new transaction.
%% Must be able to process the same transaction twice without issue.
new_transaction(S, TX) ->
	try apply_vote(S, TX) catch _:_ -> S end.

%% @doc Respond to local messages on the election server.
message(S, {get_results, PID}) ->
	PID ! {results, count_results(S#state.votes)},
	S;
message(S, {get_description, PID}) ->
	PID ! {description, S#state.desc_tx},
	S.

%%% HELPERS

%% @doc Attempt to apply a vote to the state. Fail if the TX is not a vote.
apply_vote(S = #state { name = Name }, TX) ->
	{ok, {struct, Body}} = ar_serialize:dejsonify(binary_to_list(TX#tx.data)),
	{"type", ?APP_TYPE_ID} = lists:keyfind("type", 1, Body),
	{"name", Name} = lists:keyfind("name", 1, Body),
	{"option", Opt} = lists:keyfind("option", 1, Body),
	V = #vote { address = TX#tx.owner, tx = TX#tx.id, option = Opt },
	S#state { votes = [V|(S#state.votes -- [V])] }.

%% @doc Count all votes and return results.
count_results(Votes) ->
	OptionSelections = [ V#vote.option || V <- Votes ],
	Options = ar_util:unique(OptionSelections),
	lists:sort([ {Option, ar_util:count(Option, OptionSelections)} || Option <- Options ]).
%%% FUNCTIONALITY TESTS

%% @doc Ensure the validity of a basic vote on a micro-network.
basic_election_test() ->
	ar_storage:clear(),
	% Generate wallets for each participant in the vote.
	Wallet1 = {_, Pub1} = ar_wallet:new(),
	Wallet2 = {_, Pub2} = ar_wallet:new(),
	Wallet3 = {_, Pub3} = ar_wallet:new(),
	% Create the election monitor process.
	Election = start("TestVote"),
	% Start a miner node, attached to the election monitor.
	Node =
		ar_node:start([Election],
			ar_weave:init(
				[
					{Pub1, 1000},
					{Pub2, 1090},
					{Pub3, 1000}
				]
			)
		),
	% Cast each participant's vote.
	vote(Node, Wallet1, "TestVote", 1),
	vote(Node, Wallet2, "TestVote", 2),
	vote(Node, Wallet3, "TestVote", 2),
	% Mine the votes into a block.
	ar_node:mine(Node),
	% Wait a moment for the block to mine.
	receive after 1000 -> ok end,
	% Retreive the election reults. Verify them.
	Res = results(Election),
	true = lists:member({1, 1}, Res),
	true = lists:member({2, 2}, Res).
