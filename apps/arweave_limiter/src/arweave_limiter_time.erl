%%%
%%% @doc Rate limiter clock and time management library
%%%

%%% NOTE: this module seems pretty redundant. However, moving erlang:monotonic_time/1
%%%       into this module allows us to test production code without alteration
%%%       and mock time related functions, and so manipulate and control time precisely
%%%       in tests. So here it is.

-module(arweave_limiter_time).

-export([
         ts_now/0
        ]).

ts_now() ->
    erlang:monotonic_time(millisecond).
