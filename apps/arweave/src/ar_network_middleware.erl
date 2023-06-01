-module(ar_network_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-include_lib("arweave/include/ar.hrl").

execute(Req, Env) ->
	case cowboy_req:header(<<"x-network">>, Req, <<"arweave.N.1">>) of
		<<?NETWORK_NAME>> ->
			maybe_add_peer(ar_http_util:arweave_peer(Req), Req),
			{ok, Req, Env};
		_ ->
			case cowboy_req:method(Req) of
				<<"GET">> ->
					{ok, Req, Env};
				<<"HEAD">> ->
					{ok, Req, Env};
				<<"OPTIONS">> ->
					{ok, Req, Env};
				_ ->
					wrong_network(Req)
			end
	end.

%% @doc When a node receives a request that includes the x-p2p-port header, it will attempt to
%% add the requesting node to its peer list.
maybe_add_peer(Peer, Req) ->
	case cowboy_req:header(<<"x-p2p-port">>, Req, not_set) of
		not_set ->
			ok;
		_ ->
			ar_events:send(peer, {made_request, Peer, get_release(Req)})
	end.

wrong_network(Req) ->
	{stop, cowboy_req:reply(412, #{}, jiffy:encode(#{ error => wrong_network }), Req)}.

get_release(Req) ->
	case cowboy_req:header(<<"x-release">>, Req, -1) of
		-1 ->
			-1;
		ReleaseBin ->
			case catch binary_to_integer(ReleaseBin) of
				{'EXIT', _} ->
					-1;
				Release ->
					Release
			end
	end.
