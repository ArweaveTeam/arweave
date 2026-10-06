%%% @doc Selects the host modules that throttling calls, one function per
%%% dependency. Call sites use ?DEP(Name) from
%%% include/arweave_throttling_deps.hrl.
-module(arweave_throttling_deps).

-export([
    config/0,
    serialize/0,
    metrics/0
]).

config() -> arweave_config.
serialize() -> ar_serialize.

metrics() -> arweave_metrics.
