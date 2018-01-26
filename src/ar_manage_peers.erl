-module(ar_manage_peers).
-export([update/1]).
-include("ar.hrl").

%% Update a peer list.

%% Return a new peer list, from an old one.
update(Peers) ->
	{Rankable, Newbies} = partition_newbies(score(get_more_peers(Peers))),
	maybe_drop_peers([ Peer || {Peer, _} <- rank_peers(Rankable) ]) ++ Newbies.

%% Return a new list, with the peers and their peers.
get_more_peers(Peers) ->
	ar_util:unique(
		lists:flatten(
			ar_util:pmap(fun ar_http_iface:get_peers/1, Peers)
		)
	).

%% Calculate a score for any given peer or list of peers.
score(Peers) when is_list(Peers) ->
	lists:map(fun(Peer) -> {Peer, score(Peer)} end, Peers);
score(Peer) ->
	case ar_httpc:get_performance(Peer) of
		P when P#performance.transfers < ?PEER_GRACE_PERIOD ->
			newbie;
		P -> P#performance.bytes / P#performance.time
	end.

%% Return a tuple of rankable and newbie peers.
partition_newbies(ScoredPeers) ->
	Newbies = [ P || P = {_, newbie} <- ScoredPeers ],
	{ScoredPeers -- Newbies, Newbies}.

%% Return a ranked list of peers.
rank_peers(ScoredPeers) ->
	lists:sort(fun({_, S1}, {_, S2}) -> S1 =< S2 end, ScoredPeers).

%% Probabalistically drop peers.
maybe_drop_peers(Peers) -> maybe_drop_peers(1, length(Peers), Peers).
maybe_drop_peers(Rank, NumPeers, [Peer|Peers]) when Rank =< ?MINIMUM_PEERS ->
	[Peer|maybe_drop_peers(Rank + 1, NumPeers, Peers)];
maybe_drop_peers(Rank, NumPeers, [Peer|Peers]) ->
	case roll(Rank, NumPeers) of
		true -> [Peer|maybe_drop_peers(Rank + 1, NumPeers, Peers)];
		false -> maybe_drop_peers(Rank + 1, NumPeers, Peers)
	end.

%% Generate a boolean 'drop or not' value from a rank and the number of peers.
roll(Rank, NumPeers) ->
	(rand:uniform(NumPeers - ?MINIMUM_PEERS) - 1)
		> ((Rank - ?MINIMUM_PEERS)/2).