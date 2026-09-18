%% Byte units used in protocol limits.
-define(KiB, (1024)).
-define(MiB, (1024 * ?KiB)).
-define(GiB, (1024 * ?MiB)).
-define(TiB, (1024 * ?GiB)).

%% The mainnet name. Does not change at the hard forks.
-ifndef(NETWORK_NAME).
-ifdef(AR_TEST).
-define(NETWORK_NAME, "arweave.localtest").
-else.
-define(NETWORK_NAME, "arweave.N.1").
-endif.
-endif.

%% When a request is received without specifing the X-Network header, this network name
%% is assumed.
-ifndef(DEFAULT_NETWORK_NAME).
-define(DEFAULT_NETWORK_NAME, "arweave.N.1").
-endif.

-ifdef(FORKS_RESET).
-define(FORK_1_6, 0).
-else.
%%% FORK INDEX
%%% @deprecated Fork heights from 1.7 on are defined in arweave_constants.
-define(FORK_1_6, 95000).
-endif.

%% The hashing algorithm used to calculate wallet addresses.
-define(HASH_ALG, sha256).

-define(DEEP_HASH_ALG, sha384).

-define(MERKLE_HASH_ALG, sha384).

-define(RSA_SIGN_ALG, rsa).
-define(RSA_PRIV_KEY_SZ, 4096).

-define(ECDSA_SIGN_ALG, ecdsa).
-define(ECDSA_TYPE_BYTE, <<2>>).

-define(EDDSA_SIGN_ALG, eddsa).
-define(EDDSA_TYPE_BYTE, <<3>>).

%% The default key type used by transactions that do not specify a signature type.
-define(DEFAULT_KEY_TYPE, {?RSA_SIGN_ALG, 65537}).

-define(RSA_KEY_TYPE, {?RSA_SIGN_ALG, 65537}).
-define(ECDSA_KEY_TYPE, {?ECDSA_SIGN_ALG, secp256k1}).

-define(RSA_BLOCK_SIG_SIZE, 512).
-define(ECDSA_PUB_KEY_SIZE, 33).
-define(ECDSA_SIG_SIZE, 65).

%% The difficulty a new weave is started with.
-define(DEFAULT_DIFF, 6).

-ifndef(TARGET_BLOCK_TIME).
-define(TARGET_BLOCK_TIME, 120).
-endif.

-ifndef(RETARGET_BLOCKS).
-define(RETARGET_BLOCKS, 10).
-endif.

%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is more than
%% 1.1 times bigger or smaller than ?TARGET_BLOCK_TIME * ?RETARGET_BLOCKS. Was used before
%% the fork 2.5 where we got rid of the floating point calculations.
-define(RETARGET_TOLERANCE, 0.1).

-define(JOIN_CLOCK_TOLERANCE, 15).

-define(MAX_BLOCK_PROPAGATION_TIME, 60).

-define(CLOCK_DRIFT_MAX, 5).

%% The total supply of tokens in the Genesis block.
-define(GENESIS_TOKENS, 55000000).

%% Winstons per AR.
-define(WINSTON_PER_AR, 1000000000000).

%% How far into the past or future the block can be in order to be accepted for
%% processing.
-ifdef(AR_TEST).
-define(STORE_BLOCKS_BEHIND_CURRENT, 10).
-else.
-define(STORE_BLOCKS_BEHIND_CURRENT, 50).
-endif.

%% The maximum lag when fork recovery (chain reorganisation) is performed.
-ifdef(AR_TEST).
-define(CHECKPOINT_DEPTH, 4).
-else.
-define(CHECKPOINT_DEPTH, 18).
-endif.

%% The recommended depth of the block to use as an anchor for transactions.
%% The corresponding block hash is returned by the GET /tx_anchor endpoint.
-ifdef(AR_TEST).
-define(SUGGESTED_TX_ANCHOR_DEPTH, 5).
-else.
-define(SUGGESTED_TX_ANCHOR_DEPTH, 6).
-endif.

%% The number of blocks returned in the /info 'recent' field
-ifdef(AR_TEST).
-define(RECENT_BLOCKS_WITHOUT_TIMESTAMP, 2).
-else.
-define(RECENT_BLOCKS_WITHOUT_TIMESTAMP, 5).
-endif.

%% The maximum number of tags a transaction may carry. Mirrors the
%% post-fork-2.5 limit enforced in ar_tx:validate_tags_length/2 and the
%% binary parser in ar_serialize:parse_tx_tags/1. Enforced early — at
%% JSON parse time and at tx validation entry — so we never run an
%% O(N) decode loop on a maliciously oversized list.
-define(MAX_TX_TAGS, 2048).

%% The maximum allowed size in bytes for the data field of
%% a format=1 transaction.
-define(TX_DATA_SIZE_LIMIT, 10 * ?MiB).

%% The maximum allowed size in bytes for the combined data fields of
%% the format=1 transactions included in a block. Must be greater than
%% or equal to ?TX_DATA_SIZE_LIMIT.
-define(BLOCK_TX_DATA_SIZE_LIMIT, ?TX_DATA_SIZE_LIMIT).

%% The maximum number of transactions (both format=1 and format=2) in a block.
-ifdef(AR_TEST).
-define(BLOCK_TX_COUNT_LIMIT, 10).
-else.
-define(BLOCK_TX_COUNT_LIMIT, 1000).
-endif.

%% The base transaction size the transaction fee must pay for.
-define(TX_SIZE_BASE, 3210).

%% The adjustment of difficutly going from SHA-384 to RandomX.
-define(RANDOMX_DIFF_ADJUSTMENT, (-14)).

%% Max allowed difficulty multiplication and division factors, before the fork 2.4.
-define(DIFF_ADJUSTMENT_DOWN_LIMIT, 2).
-define(DIFF_ADJUSTMENT_UP_LIMIT, 4).

%% Maximum size of a single data chunk, in bytes.
-define(DATA_CHUNK_SIZE, (256 * 1024)).

%% The maximum allowed packing difficulty.
%% The number of sub-chunks in a packed chunk.

-define(SUB_CHUNK_COUNT, 32).

%% The size of a unit sub-chunk in a packed chunk.
-define(SUB_CHUNK_SIZE,
    (?DATA_CHUNK_SIZE div ?SUB_CHUNK_COUNT)
).

%% Maximum size of a `data_path`, in bytes.
-define(MAX_PATH_SIZE, (256 * 1024)).

%% The size of data chunk hashes, in bytes.
-define(CHUNK_ID_HASH_SIZE, 32).

-define(NOTE_SIZE, 32).

%% The speed in chunks/s of moving the fork 2.5 packing threshold.
-ifdef(AR_TEST).
-define(PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND, 1).
-else.
-define(PACKING_2_5_THRESHOLD_CHUNKS_PER_SECOND, 10).
-endif.

%% The data_root of the system "padding" nodes inserted in the transaction Merkle trees
%% since the 2.5 fork block. User transactions cannot set <<>> for data_root unless
%% data_size == 0. The motivation is to place all chunks including those
%% smaller than 256 KiB into the 256 KiB buckets on the weave, to even out their chances to be
%% picked as recall chunks and therefore equally incentivize the storage.
-define(PADDING_NODE_DATA_ROOT, <<>>).

-ifndef(INITIAL_VDF_DIFFICULTY).
-define(INITIAL_VDF_DIFFICULTY, 600_000).
-endif.

%% A macro to convert AR into Winstons.
-define(AR(AR), (?WINSTON_PER_AR * AR)).

%% The messages to be stored inside the genesis block.
-define(GENESIS_BLOCK_MESSAGES, []).
