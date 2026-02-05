%%%
%%% @doc Cowboy handler to manage server-side rate limiting.
%%%
%%% This module provides a routing layer, mapping incoming requests
%%% to respective rate limiter groups (RLG). 
%%% The mapping logic can be extended in a quite complex manner if 
%%% required, however it should be  considered that the execute function will be
%%% called for each HTTP request.
%%% 
%%% Also, there is nothing limiting the developer from calling multiple RLGs
%%% for a single request, if necessary.
%%%
%%% The LimiterRef reference  in the arweave_limiter:register_or_reject_call/2
%%% call must match one of the RLGs started by the arweave_limiter application,
%%% otherwise a noproc error will be raised.
%%% 
%%% We currency use IP addresses and ports as Keys for the calling peers. 
%%% However, any Erlang term might be used as a key in an RLG.
%%% 
-module(ar_http_iface_rate_limiter_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

execute(Req, Env) ->
	LimiterRef = get_limiter_ref(Req),
	PeerKey = get_peer_key(Req),

	case arweave_limiter:register_or_reject_call(LimiterRef, PeerKey) of
		{reject, Reason, Data} ->
			?LOG_ERROR([{event, rate_limiter_reject}, {reason, Reason}, {data, Data}]),
			{stop, reject(Req, Reason, Data)};
		_ ->
			{ok, Req, Env}
	end.

get_limiter_ref(Req) ->
	{ok, Config} = arweave_config:get_env(),
	LocalIPs = [config_peer_to_ip_addr(Peer) || Peer <- Config#config.local_peers],
	PeerIP = config_peer_to_ip_addr(get_peer_key(Req)),

	case lists:member(PeerIP, LocalIPs) of
		true ->
			local_peers;
		_ ->
			Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
			path_to_limiter_ref(Path)
	end.

reject(Req, _Reason, _Data) ->
	cowboy_req:reply(
		429,
		#{},
		<<"Too Many Requests">>,
		Req
	).

get_peer_key(Req) ->
	{{A, B, C, D}, _Port} = cowboy_req:peer(Req),
	{A, B, C, D}.

config_peer_to_ip_addr({{A, B, C, D}, _Port}) -> {A, B, C, D};
config_peer_to_ip_addr({A, B, C, D, _Port}) -> {A, B, C, D};
config_peer_to_ip_addr({A, B, C, D}) -> {A, B, C, D}.

path_to_limiter_ref([<<"chunk">> | _]) -> chunk;
path_to_limiter_ref([<<"chunk2">> | _]) -> chunk;
path_to_limiter_ref([<<"data_sync_record">> | _]) -> data_sync_record;
path_to_limiter_ref([<<"recent_hash_list_diff">> | _]) -> recent_hash_list_diff;
path_to_limiter_ref([<<"hash_list">>]) -> block_index;
path_to_limiter_ref([<<"hash_list2">>]) -> block_index;
path_to_limiter_ref([<<"block_index">>]) -> block_index;
path_to_limiter_ref([<<"block_index2">>]) -> block_index;
path_to_limiter_ref([<<"block">>, _Type, _ID, <<"hash_list">>]) -> block_index;
path_to_limiter_ref([<<"wallet_list">>]) -> wallet_list;
path_to_limiter_ref([<<"block">>, _Type, _ID, <<"wallet_list">>]) -> wallet_list;
path_to_limiter_ref([<<"vdf">>]) -> get_vdf;
path_to_limiter_ref([<<"vdf">>, <<"session">>]) -> get_vdf_session;
path_to_limiter_ref([<<"vdf2">>, <<"session">>]) -> get_vdf_session;
path_to_limiter_ref([<<"vdf3">>, <<"session">>]) -> get_vdf_session;
path_to_limiter_ref([<<"vdf4">>, <<"session">>]) -> get_vdf_session;
path_to_limiter_ref([<<"vdf">>, <<"previous_session">>]) -> get_previous_vdf_session;
path_to_limiter_ref([<<"vdf2">>, <<"previous_session">>]) -> get_previous_vdf_session;
%% No vdf3 prev_session in ar_blacklist_middleware.hrl ?RPM_BY_PATH
path_to_limiter_ref([<<"vdf4">>, <<"previous_session">>]) -> get_previous_vdf_session;
path_to_limiter_ref([<<"metrics">> | _ ])-> metrics;
path_to_limiter_ref(_) -> general.
