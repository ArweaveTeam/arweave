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
            case Path of
                [<<"metrics">> | _ ] ->
                    metrics;
                _ ->
                    general
            end
    end.

reject(Req, _Reason, _Data) ->
    cowboy_req:reply(
      429,
      #{
        %% TODO: FILL IN these fields in a reasonable manner
        %<<"connection">> => <<"close">>,
        %<<"retry-after">> => integer_to_binary(?THROTTLE_PERIOD div 1000),
        %<<"x-rate-limit-limit">> => integer_to_binary(Limit)
       },
      <<"Too Many Requests">>,
      Req
     ).

get_peer_key(Req) ->
    {{A, B, C, D}, Port} = cowboy_req:peer(Req),
    {A, B, C, D, Port}.

config_peer_to_ip_addr({A, B, C, D, _}) -> {A, B, C, D}.
