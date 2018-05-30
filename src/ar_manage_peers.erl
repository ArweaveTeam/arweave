-module(ar_manage_peers).
-export([update/1, stats/0]).
-include("ar.hrl").

%%% Manage and update peer lists.

%% @doc Print statistics about the current peers.
stats() ->
	Connected = ar_bridge:get_remote_peers(http_bridge_node),
	All = [ Peer || {peer, Peer} <- ar_meta_db:keys() ],
	io:format("Connected peers, in preference order:~n"),
	stats(Connected),
	io:format("Other known peers:~n"),
	stats(All).
stats(Peers) ->
	lists:foreach(
		fun(Peer) -> format_stats(Peer, ar_httpc:get_performance(Peer)) end,
		Peers
	).

%% @doc Pretty print stats about a node.
format_stats(Peer, Perf) ->
	io:format("\t~s ~.2f kb/s (~p transfers)~n",
		[
			string:pad(ar_util:format_peer(Peer), 20, trailing, $ ),
			(Perf#performance.bytes / 1024) / (Perf#performance.time / 1000000),
			Perf#performance.transfers
		]
	).

%% @doc Take an existing peer list and create a new peer list. Gets all current peers
%% peerlist and ranks each peer by its connection speed to this node in the past.
%% Peers who have behaved well in the past are favoured in ranking.
%% New, unknown peers are given 100 blocks of grace.
update(Peers) ->
	ar_meta_db:remove_old(os:system_time(seconds)),
	{Rankable, Newbies} = partition_newbies(score(get_more_peers(Peers))),
	NewPeers = (lists:sublist(maybe_drop_peers([ Peer || {Peer, _} <- rank_peers(Rankable) ])
		++ [ Peer || {Peer, newbie} <- Newbies ], ?MAXIMUM_PEERS)),
	lists:foreach(
		fun(P) ->
			case lists:member(P, NewPeers) of
				false -> ar_httpc:update_timer(P);
				_ -> ok
			end
		end,
		Peers	
	),
	NewPeers.

%% @doc Return a new list, with the peers and their peers.
get_more_peers(Peers) ->
	ar_util:unique(
		lists:flatten(
			[
				ar_util:pmap(fun ar_http_iface:get_peers/1, Peers),
				Peers
			]
		)
	).

%% @doc Calculate a rank order for any given peer or list of peers.
score(Peers) when is_list(Peers) ->
	lists:map(fun(Peer) -> {Peer, score(Peer)} end, Peers);
score(Peer) ->
	case ar_httpc:get_performance(Peer) of
		P when P#performance.transfers < ?PEER_GRACE_PERIOD ->
			newbie;
		P -> P#performance.bytes / P#performance.time
	end.

%% @doc Given a set of peers, returns a tuple containing peers that
%% are "rankable" and elidgible to be pruned, and new peers who are
%% within their grace period who are not
partition_newbies(ScoredPeers) ->
	Newbies = [ P || P = {_, newbie} <- ScoredPeers ],
	{ScoredPeers -- Newbies, Newbies}.

%% @doc Return a ranked list of peers.
rank_peers(ScoredPeers) ->
	lists:sort(fun({_, S1}, {_, S2}) -> S1 >= S2 end, ScoredPeers).

%% @doc Probabalistically drop peers based on their rank. Highly ranked peers are
%% less likely to be dropped than lower ranked ones.
maybe_drop_peers(Peers) -> maybe_drop_peers(1, length(Peers), Peers).
maybe_drop_peers(_, _, []) -> [];
maybe_drop_peers(Rank, NumPeers, [Peer|Peers]) when Rank =< ?MINIMUM_PEERS ->
	[Peer|maybe_drop_peers(Rank + 1, NumPeers, Peers)];
maybe_drop_peers(Rank, NumPeers, [Peer|Peers]) ->
	case roll(Rank, NumPeers) of
		true -> [Peer|maybe_drop_peers(Rank + 1, NumPeers, Peers)];
		false -> maybe_drop_peers(Rank + 1, NumPeers, Peers)
	end.

%% @doc Generate a boolean 'drop or not' value from a rank and the number of peers.
roll(Rank, NumPeers) ->
	case Rank =< ?MINIMUM_PEERS of
		true -> true;
		false -> 
			(2 * rand:uniform(NumPeers - ?MINIMUM_PEERS)) >=
				(Rank - ?MINIMUM_PEERS)
	end.
