-module(arweave_config_options_client_throttling).
-behaviour(arweave_config_options).
-export([
	specs/0,
	group_description/0,
	validate/0,
	group_ids/0
]).

specs() -> [].

group_description() ->
	<<"HTTP API rate-limiter groups — sliding window + leaky bucket "
	  "+ concurrency caps.">>.

validate() ->
    ok.

type_for(_) -> pos_integer.

group_ids() ->
    [].
