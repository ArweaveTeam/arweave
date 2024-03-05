-define(OFFSET_SIZE, 3). % Sufficient to represent a number up to 256 * 1024 (?DATA_CHUNK_SIZE).
-define(OFFSET_BIT_SIZE, (?OFFSET_SIZE * 8)).

-define(CHUNK_DIR, "chunk_storage").
