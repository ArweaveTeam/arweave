-module(ar_blacklist).
-export([start/0]).
-export([increment_ip/1, is_blacklisted/1, maybe_blacklist_ip/1, unblacklist_ip/1]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Defines a small in-memory metadata table for Archain nodes.
%%% Typically used to store small peices of globally useful information
%%% (for example: the port number used by the node).

%% @doc Initialise the metadata storage service.
start() ->
	spawn(
		fun() ->
			ar:report([starting_blacklist_db]),
			ets:new(?MODULE, [set, public, named_table]),
			ets:new(list, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.

increment_ip(Peer) ->
	case ets:lookup(?MODULE, Peer) of
		[{Peer, Counter}] -> ets:insert(?MODULE, {Peer, Counter+1});
		[] -> 
			ets:insert(?MODULE, {Peer, 1}),
			timer:apply_after(5000, ?MODULE, maybe_blacklist_ip, [Peer])
	end.

maybe_blacklist_ip(Peer) ->
	case ets:lookup(?MODULE, Peer) of
		[{Peer, Counter}] ->
			if 
				(Counter > ?MAX_REQUESTS) ->
					ets:delete(?MODULE, Peer),
					blacklist_ip(Peer);
				true -> ets:delete(?MODULE, Peer)
			end;
		_ -> ok
	end.

blacklist_ip(Peer) ->
	case ets:lookup(list, Peer) of
		[{Peer, 0}] -> ok;
		_ ->
			ar:report(
				[
					{blacklisting_peer, Peer},
					{too_many_requests}
				]
			),
			ets:insert(list, {Peer, 0}),
			timer:apply_after(30000, ?MODULE, unblacklist_ip, [Peer])
	end.

unblacklist_ip(Peer) ->
	ar:report(
		[
			{unblacklisting_peer, Peer},
			{timeout_complete}
		]
	),
	ets:delete(?MODULE, Peer),
	ets:delete(list, Peer).

is_blacklisted(Peer) ->
	case ets:lookup(list, Peer) of
		[{Peer, 0}] -> true;
		_ -> false
	end.
