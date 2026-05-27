%%% @doc CLI-facing facade for `./bin/arweave config get/set`.
%%%
%%% Parses the user-facing string key — dotted (`mining.address`) or
%%% single-segment (`debug`) — into the canonical option_key, then
%%% delegates to `arweave_config:get/1` or `arweave_config:set/2`.
%%%
%%% Returns raw Erlang terms so `erl_call -a` renders them straight to
%%% the operator's terminal.
-module(arweave_config_cli).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/1]}).
-export([get/1, set/2]).

%% @doc Read a configuration value by its canonical key in dotted
%% form (or as a single segment). Returns the value, `undefined` when
%% the key is not registered, or `{error, Reason}` on a malformed key.
-spec get(string()) -> term() | undefined | {error, term()}.
get(StringKey) ->
    case arweave_config_parser:key(StringKey) of
        {ok, Key} -> arweave_config:get(Key);
        {error, _} = Err -> Err
    end.

%% @doc Set a configuration value by its canonical key. The value is
%% passed through the spec's type coercion. Returns `{ok, NewValue}`
%% or `{error, Reason}`.
-spec set(string(), string()) -> {ok, term()} | {error, term()}.
set(StringKey, StringValue) ->
    case arweave_config_parser:key(StringKey) of
        {ok, Key} -> arweave_config:set(Key, StringValue);
        {error, _} = Err -> Err
    end.
