-ifndef(AR_MINING_CACHE_HRL).
-define(AR_MINING_CACHE_HRL, true).

-record(ar_mining_cache_value, {
  chunk1 :: binary() | undefined,
  chunk2 :: binary() | undefined,
  chunk2_missing = false :: boolean(),
  h1 :: binary() | undefined,
  h2 :: binary() | undefined
}).

-record(ar_mining_cache_session, {
  mining_cache = #{} :: #{term() => #ar_mining_cache_value{}},
  mining_cache_size_bytes = 0 :: non_neg_integer(),
  reserved_mining_cache_bytes = 0 :: non_neg_integer()
}).


-record(ar_mining_cache, {
  mining_cache_sessions = #{} :: #{term() => #ar_mining_cache_session{}},
  mining_cache_sessions_queue = queue:new() :: queue:queue(),
  mining_cache_limit_bytes = 0 :: non_neg_integer()
}).

-endif.
