-record(ar_chunk_cache_group, {
  chunk_cache = #{} :: #{term() => binary()},
  chunk_cache_size_bytes = 0 :: non_neg_integer(),
  reserved_chunk_cache_bytes = 0 :: non_neg_integer()
}).


-record(ar_chunk_cache, {
  chunk_cache_groups = #{} :: #{term() => #ar_chunk_cache_group{}},
  chunk_cache_limit_bytes = 0 :: non_neg_integer()
}).
