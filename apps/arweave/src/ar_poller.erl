-module(ar_poller).
-export([start/2]).
-include("ar.hrl").

%%% This module spawns a process that regularly checks for updates to
%%% the current block and returns it if a new one is found.

%% The time to poll peers for a new current block.
-define(POLL_TIME, 10*1000).

%% @doc Starts poll server.
start(Node, Peers) ->
	spawn(
		fun() ->
			server(Node, Peers)
		end
	).

%% @doc Regularly poll peers for a new block.
server(Node, Peers) -> server(Node, Peers, no_block_hash).
server(Node, Peers, LastBH) ->
	receive after ?POLL_TIME -> ok end,
	case get_remote_block_hash_list(Peers) of
		unavailable ->
			server(Node, Peers, LastBH);
		{[LastBH | _], _} ->
			server(Node, Peers, LastBH);
		{BHL, Peer} ->
			case get_remote_block_pair(Peer, BHL) of
				{B, PreviousRecallB} ->
					Recall = {PreviousRecallB#block.indep_hash, PreviousRecallB#block.block_size, <<>>, <<>>},
					Node ! {new_block, Peer, B#block.height, B, no_data_segment, Recall},
					server(Node, Peers, hd(BHL));
				unavailable ->
					server(Node, Peers, LastBH)
			end
	end.

get_remote_block_hash_list([]) ->
	unavailable;
get_remote_block_hash_list([Peer | Peers]) ->
	case catch ar_http_iface_client:get_hash_list(Peer) of
		{'EXIT', _} ->
			get_remote_block_hash_list(Peers);
		BHL ->
			{BHL, Peer}
	end.

get_remote_block_pair(Peer, BHL) ->
	case ar_node_utils:get_full_block(Peer, hd(BHL), BHL) of
		unavailable -> unavailable;
		B -> get_remote_block_pair(Peer, BHL, B)
	end.

get_remote_block_pair(Peer, BHL, B) ->
	%% Fetch the previous block, since that's required before calling
	%% ar_util:get_recall_hash/2
	case get_block(B#block.previous_block, BHL, Peer) of
		{ok, _} ->
			PreviousRecallBH = ar_util:get_recall_hash(B#block.previous_block, BHL),
			case get_block(PreviousRecallBH, BHL, Peer) of
				{ok, PreviouRecallB} -> {B, PreviouRecallB};
				unavailable -> unavailable
			end;
		unavailable ->
			unavailable
	end.

get_block(BH, BHL, Peer) ->
	case ar_storage:read_block(BH, BHL) of
		unavailable ->
			get_block_remote(BH, BHL, Peer);
		B ->
			{ok, B}
	end.

get_block_remote(BH, BHL, Peer) ->
	case ar_node_utils:get_full_block(Peer, BH, BHL) of
		unavailable -> unavailable;
		not_found -> unavailable;
		B ->
			ar_storage:write_full_block(B),
			{ok, B}
	end.
