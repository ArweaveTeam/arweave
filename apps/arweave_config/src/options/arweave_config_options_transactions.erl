%%% @doc Specs for the `transactions` option group. Block- and
%%% allow-listing of transactions and chunks.
-module(arweave_config_options_transactions).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [
        #{
            enabled => true,
            option_key => [transactions, blocklist, files],
            runtime => true,
            default => [],
            type => list,
            legacy => transaction_blacklist_files,
            short_description =>
                <<"A file containing blocklisted transactions.">>,
            long_description =>
                <<"One Base64 encoded transaction ID per line.">>
        },
        #{
            enabled => true,
            option_key => [transactions, blocklist, urls],
            runtime => true,
            default => [],
            type => list,
            legacy => transaction_blacklist_urls,
            short_description =>
                <<"An HTTP endpoint serving a transaction blocklist.">>
        },
        #{
            enabled => true,
            option_key => [transactions, allowlist, files],
            runtime => true,
            default => [],
            type => list,
            legacy => transaction_whitelist_files,
            short_description =>
                <<"A file containing allowlisted transactions.">>,
            long_description =>
                <<"One Base64 encoded transaction ID per line. If a "
                  "transaction is in both lists, it is considered "
                  "allowlisted.">>
        },
        #{
            enabled => true,
            option_key => [transactions, allowlist, urls],
            runtime => true,
            default => [],
            type => list,
            legacy => transaction_whitelist_urls,
            short_description =>
                <<"An HTTP endpoint serving a transaction allowlist.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Manage transaction allowlist and blocklist behavior.">>.
