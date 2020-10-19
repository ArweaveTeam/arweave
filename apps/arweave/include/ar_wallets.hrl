%% @doc The maximum number of wallets served via /wallet_list/<root_hash>[/<cursor>].
-ifdef(DEBUG).
-define(WALLET_LIST_CHUNK_SIZE, 2).
-else.
-define(WALLET_LIST_CHUNK_SIZE, 2500).
-endif.

%% @doc The upper limit for the size of the response fetched from
%% /wallet_list/<root_hash>[/<cursor>], when serialized using Erlang Term Format.
%% The actual size of the binary for so many wallets is a few kilobytes smaller,
%% so the response may contain some metadata.
-define(MAX_SERIALIZED_WALLET_LIST_CHUNK_SIZE, ?WALLET_LIST_CHUNK_SIZE * 202). % = 505000
