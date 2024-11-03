-ifndef(AR_POA_HRL).
-define(AR_POA_HRL, true).

-record(chunk_proof, {
	absolute_offset :: non_neg_integer(),
	tx_root :: binary(),
	tx_path :: binary(),
	data_root :: binary(),
	data_path :: binary(),
	tx_start_offset :: non_neg_integer(),
	tx_end_offset :: non_neg_integer(),
	block_start_offset :: non_neg_integer(),
	block_end_offset :: non_neg_integer(),
	chunk_id :: binary(),
	chunk_start_offset :: non_neg_integer(),
	chunk_end_offset :: non_neg_integer(),
	validate_data_path_ruleset :: 
		'offset_rebase_support_ruleset' |
		'strict_data_split_ruleset' |
		'strict_borders_ruleset',
	tx_path_is_valid = not_validated :: 'not_validated' | 'valid' | 'invalid',
	data_path_is_valid = not_validated :: 'not_validated' | 'valid' | 'invalid',
	chunk_is_valid = not_validated :: 'not_validated' | 'valid' | 'invalid'
}).

-endif.
