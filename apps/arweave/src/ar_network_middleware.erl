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

maybe_add_peer(Peer, Req) ->
	case cowboy_req:header(<<"x-p2p-port">>, Req, not_set) of
		not_set ->
			ok;
		_ ->
			case ar_meta_db:get({peer, Peer}) of
				not_found ->
					ar_meta_db:put({peer, Peer}, #performance{}),
					ar_bridge:add_remote_peer(Peer);
				_ ->
					ok
			end
	end.

wrong_network(Req) ->
	{stop, cowboy_req:reply(412, #{}, jiffy:encode(#{ error => wrong_network }), Req)}.

