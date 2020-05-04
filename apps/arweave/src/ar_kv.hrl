
-define(ROCKSDB_OPTIONS, [{create_if_missing, true}, {prefix_extractor, {capped_prefix_transform, 28}}]).
-define(ROCKSDB_ITR_OPTIONS, []).