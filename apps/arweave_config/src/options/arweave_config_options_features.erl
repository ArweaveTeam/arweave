%%% @doc Specs for the `features` option group. Options for
%%% gating optional or transitional node behavior.
%%%
%%% Feature flags are declared in `arweave_config_features:catalog/0`.
%%% This module turns each catalog entry into a boolean config option at
%%% `[features, Name]`. The option inherits its default and description
%%% from the catalog entry.
%%%
%%% Legacy `enable: [...]` / `disable: [...]` list inputs (from the
%%% legacy CLI and legacy `config.json`) are classified per-entry by
%%% `arweave_config_features:classify_legacy_flag/2` at parse time —
%%% promoted flags get written to their dedicated spec field, catalog
%%% flags become `[features, Name] = true|false`, tombstoned and
%%% unknown entries are warned and dropped. No staging keys live in
%%% the options registry.
%%%
%%% Feature-name validator (`arweave_config_features:validate/0`)
%%% rejects unknown names under `[features, ...]`.
-module(arweave_config_options_features).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-include("arweave_config.hrl").

specs() ->
    [build_per_flag_spec(Entry)
        || Entry <- arweave_config_features:catalog()].

build_per_flag_spec(#{ name := Name,
                       default := Default,
                       description := Description } = Entry) ->
    #{
        enabled => true,
        option_key => [features, Name],
        runtime => maps:get(runtime, Entry, false),
        default => Default,
        type => boolean,
        short_description => Description
    }.

validate() ->
    arweave_config_features:validate().

group_description() ->
    <<"Manage optional node feature flags.">>.
