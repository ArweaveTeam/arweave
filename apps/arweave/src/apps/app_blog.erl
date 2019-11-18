-module(app_blog).
-export([start/1, message/3, new_post/3]).

start(Peers) ->
	{PrivKey, PubKey} = ar_wallet:new(),
	PID = adt_simple:start(?MODULE, PubKey, Peers),
	{PrivKey, PID}.

message(PubKey, GossipS, {new_post, PrivKey, Post}) ->
	T = ar_tx:new(Post),
	SignedT = ar_tx:sign(T, PrivKey, PubKey),
	GossipS2 = ar_gossip:send(GossipS, {add_tx, SignedT}),
	{PubKey, GossipS2}.

new_post(PID, PrivKey, Text) ->
	PID ! {new_post, PrivKey, Text}.
