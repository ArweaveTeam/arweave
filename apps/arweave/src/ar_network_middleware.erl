-module(ar_network_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-include("ar.hrl").

execute(Req, Env) ->
	case cowboy_req:header(<<"x-network">>, Req, <<"arweave.N.1">>) of
		<<?NETWORK_NAME>> ->
			maybe_add_peer(ar_http_util:arweave_peer(Req)),
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

maybe_add_peer(Peer) ->
	case ar_meta_db:get({peer, Peer}) of
		not_found ->
			ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer);
		_ ->
			ok
	end.

wrong_network(Req) ->
	{stop, cowboy_req:reply(412, #{}, jiffy:encode(#{ error => wrong_network }), Req)}.

