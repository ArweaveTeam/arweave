-ifndef(AR_VERIFY_CHUNKS_HRL).
-define(AR_VERIFY_CHUNKS_HRL, true).

-record(verify_report, {
	start_time :: non_neg_integer(),
	total_error_bytes = 0 :: non_neg_integer(),
	total_error_chunks = 0 :: non_neg_integer(),
	error_bytes = #{} :: #{atom() => non_neg_integer()},
	error_chunks = #{} :: #{atom() => non_neg_integer()},
	bytes_processed = 0 :: non_neg_integer(),
	progress = 0 :: non_neg_integer(),
	status = not_ready :: not_ready | running| done
}).

-endif.
