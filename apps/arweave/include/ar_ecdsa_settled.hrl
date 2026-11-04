%% @doc The maximum number of entries served via /ecdsa_settled/<cursor>.
-ifdef(AR_TEST).
-define(ECDSA_SETTLED_CHUNK_SIZE, 2).
-else.
-define(ECDSA_SETTLED_CHUNK_SIZE, 16384).
-endif.

%% @doc The size of a serialized entry: a 32-byte transaction identifier, the
%% 8-byte height of the block it was settled from, and the 32-byte SHA-256 hash
%% of its signature data segment.
-define(ECDSA_SETTLED_ENTRY_SIZE, 72).

%% @doc The upper limit for the size of the response fetched from
%% /ecdsa_settled/<cursor>.
-define(MAX_SERIALIZED_ECDSA_SETTLED_CHUNK_SIZE,
        ?ECDSA_SETTLED_CHUNK_SIZE * ?ECDSA_SETTLED_ENTRY_SIZE). % = 1179648
