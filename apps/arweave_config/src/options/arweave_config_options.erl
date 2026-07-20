%%% @doc Behaviour for arweave_config option group modules.
%%%
%%% Each module under `src/options/` owns one logical option group,
%%% such as `mining`, `peers`, or `storage_modules`. The group module
%%% describes the option_keys it owns by returning spec maps from
%%% `specs/0`. The spec registry uses those maps to parse values,
%%% provide defaults, connect legacy names, and route reads and writes
%%% through optional callbacks.
%%%
%%% Option group modules are listed in
%%% `arweave_config_options_spec:option_modules/0`. That list is the
%%% roadmap for this part of the app: it defines which groups exist,
%%% their help-output order, and the order in which group validators
%%% run.
%%%
%%% == Callbacks ==
%%%
%%% `specs/0` — returns a list of option spec maps. Called once
%%% during spec assembly by `arweave_config_options_spec:all/0`.
%%%
%%% `validate/0` — checks invariants that involve more than one leaf
%%% in the group, or that depend on another option group. Returns `ok`
%%% if the assembled config is consistent, `{error, Reason}` otherwise,
%%% where `Reason` is a binary or string describing the problem in
%%% operator-friendly terms.
%%%
%%% Validators are run fail-fast in `option_modules/0` order. The first
%%% `{error, _}` aborts the validation pass and is returned to the
%%% caller of `arweave_config_validate:run/0`.
%%%
%%% Modules with no group-level invariants implement `validate/0` as
%%% `validate() -> ok.`
%%%
%%% Modules with no options and no validators don't need to exist;
%%% don't add an empty option group module.
%%%
%%% == Adding or changing a group ==
%%%
%%% Add or update the group module in this directory, then include it
%%% in `arweave_config_options_spec:option_modules/0`. Keep the module
%%% focused on the option group it names. If a validation rule needs
%%% state from another group, call that out in the function comment
%%% near the validator.
%%%
-module(arweave_config_options).

-callback specs() -> [map()].

-callback validate() -> ok | {error, Reason} when
    Reason :: binary() | string().

%% Optional callback. Returns the group atom for this option module.
%% When omitted, the group is derived from the module name by
%% stripping the `arweave_config_options_` prefix (so
%% `arweave_config_options_sync` becomes `sync`). The help printer
%% uses the group to cluster related options under a section
%% header.
-callback group() -> atom().

%% Optional callback. Returns a one-paragraph description shown
%% beneath the group header in the help output. Omit (don't export)
%% when no description is needed.
-callback group_description() -> binary().

%% Optional callback. Post-parse normalization: promote legacy shapes,
%% mirror dependent flags, fill derived fields, or otherwise massage
%% the loaded store before validation. Runs once at boot from
%% `arweave_config_normalize:run/0` in `option_modules/0` order, after
%% all input sources have been parsed and before `validate/0` runs.
%% Return value is ignored. Exceptions are not caught by the
%% normalization walker and abort the normalization pass.
-callback normalize() -> ok.

-optional_callbacks([group/0, group_description/0, normalize/0]).
