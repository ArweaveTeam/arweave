%%% @doc Selects the host modules that limiter calls, one function per
%%% dependency. Call sites use ?DEP(Name) from include/arweave_limiter_deps.hrl.
-module(arweave_limiter_deps).

-export([
    config/0
]).

config() -> arweave_config.
