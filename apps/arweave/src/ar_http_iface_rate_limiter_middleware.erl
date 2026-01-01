-module(ar_http_iface_rate_limiter_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).

-include_lib("arweave/include/ar.hrl").

execute(Req, Env) ->
    LimiterRef = get_limiter_ref(Req),
    IPAddr = requesting_ip_addr(Req),
    
    case arweave_limiter:register_or_reject_call(LimiterRef, IPAddr) of
        {reject, Reason, Data} ->
            {stop, reject(Req, Reason, Data)};
        _ ->
            {ok, Req, Env}
    end.

get_limiter_ref(Req) ->
    Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
    %% FIXME: Perhaps this could be part of the config, and generated dynamically
    case Path of
        [<<"metrics">> | _ ] ->
            metrics;
        _ ->
            general
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

requesting_ip_addr(Req) ->
	{IPAddr, _} = cowboy_req:peer(Req),
	IPAddr.
