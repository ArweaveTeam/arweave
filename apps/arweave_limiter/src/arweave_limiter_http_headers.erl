%%%
%%% @doc Turns a HeadersInfo map (returned by
%%% arweave_limiter_group:register_or_reject_call/2) into RateLimit-* HTTP
%%% response headers per draft-polli-ratelimit-headers-02.
%%%
%%% The "expiring-limit" — the first value of RateLimit-Limit — tracks
%%% whichever policy is closest to its limit, switching between the
%%% sliding window and the leaky bucket as the request flow progresses.
%%%
-module(arweave_limiter_http_headers).

-export([to_http_headers/1, ratelimit_limit_value/1]).

%% Returns a list of {HeaderName, Value} tuples ready to attach to the
%% response. Returns [] in disabled mode (no headers advertised).
to_http_headers({register, no_limiting_applied, _Info}) ->
    #{};
to_http_headers({RegOrRej, _Mode, #{expiring_limit := _ExpiringLimit,
                                    remaining      := Remaining,
                                    reset_seconds  := Reset} = HeadersInfo}) ->
    Headers = #{<<"RateLimit-Limit">> => ratelimit_limit_value(HeadersInfo),
                <<"RateLimit-Remaining">> => integer_to_binary(Remaining),
                <<"RateLimit-Reset">> =>     integer_to_binary(Reset)},
    maybe_add_retry_after(RegOrRej, Remaining, Reset, Headers).

%% RateLimit-Limit = expiring-limit *( "," quota-policy )
%%
%% expiring-limit = the limit closest to being reached.
%% Trailing quota-policy items describe each underlying policy with `w=` and
%% the `policy=` quota-comment, so clients aware of multiple policies can
%% see both the sliding window and the leaky bucket.
ratelimit_limit_value(#{expiring_limit := Expiring,
                        policies := #{concurrency := #{limit := ConcurrencyLimit},
                                      sliding_window := Sw,
                                      leaky_bucket   := Lb}}) ->
    SwLimit  = maps:get(limit, Sw),
    SwWindow = maps:get(window_seconds, Sw),
    LbBurst  = maps:get(burst, Lb),
    iolist_to_binary(
      io_lib:format(
        "~B, ~B;w=~B;policy=\"sliding window\", "
        "~B;w=~B;burst=~B;policy=\"leaky bucket\" "
        "~B;w=~B;policy=\"concurrency\" ",
        [Expiring, SwLimit, SwWindow, LbBurst, SwWindow, LbBurst, ConcurrencyLimit, 1])).

%% When both Retry-After and RateLimit-Reset are present they
%% should reference the same instant. We add Retry-After only on rejects
%% so well-behaved clients back off.
maybe_add_retry_after(reject, 0, Reset, HeaderMap) ->
    HeaderMap#{<<"Retry-After">> => integer_to_binary(Reset)};
maybe_add_retry_after(_, _, _, HeaderMap) ->
    HeaderMap.
