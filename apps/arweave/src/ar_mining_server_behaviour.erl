-module(ar_mining_server_behaviour).

-include("ar.hrl").
-include("ar_mining.hrl").

-callback start_mining({DiffPair, MerkleRebaseThreshold, Height}) -> ok when
	DiffPair :: {non_neg_integer() | infinity, non_neg_integer() | infinity},
	MerkleRebaseThreshold :: non_neg_integer() | infinity,
	Height :: non_neg_integer().

-callback pause() -> ok.

-callback is_paused() -> boolean().

-callback set_difficulty(DiffPair :: {non_neg_integer() | infinity, non_neg_integer() | infinity}) -> ok.

-callback set_merkle_rebase_threshold(Threshold :: non_neg_integer() | infinity) -> ok.

-callback set_height(Height :: integer()) -> ok.
