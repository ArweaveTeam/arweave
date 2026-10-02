%%% @doc Specs for the `throttling` option group. Options for the
%%% client-side outbound request throttler (`arweave_throttling').
-module(arweave_config_options_throttling).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).

-define(DEFAULT_THROTTLING_IDLE_TIMEOUT_MS, 60000).
-define(DEFAULT_THROTTLING_MAX_PROCESSES, 1000).

specs() ->
    [
        #{
            enabled => true,
            option_key => [throttling, idle_timeout],
            runtime => true,
            default => ?DEFAULT_THROTTLING_IDLE_TIMEOUT_MS,
            type => non_neg_integer,
            short_description =>
                <<"Milliseconds a throttling group process may stay "
                  "idle before it shuts itself down.">>,
            long_description =>
                <<"A throttling group is idle when it has received no "
                  "throttle, is_throttled or quota update request, has "
                  "no queued callers and no pending quota refill. The "
                  "group is started again on the next quota update. "
                  "Running groups pick up a change at their next idle "
                  "check.">>
        },
        #{
            enabled => true,
            option_key => [throttling, max_processes],
            runtime => true,
            default => ?DEFAULT_THROTTLING_MAX_PROCESSES,
            type => non_neg_integer,
            short_description =>
                <<"Maximum number of running throttling group "
                  "processes.">>,
            long_description =>
                <<"A quota update for a group without a running "
                  "process is rejected with `process_limit_breached` "
                  "once this many group processes are running. Groups "
                  "stopped after being idle and groups also defined "
                  "for the local limiter do not count and are never "
                  "rejected.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Tune the client-side outbound request throttler.">>.
