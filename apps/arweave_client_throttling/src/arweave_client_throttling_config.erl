%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @doc Configuration helpers for `arweave_client_throttling'.
%%%
%%% The application reads the list of throttling groups from the
%%% `groups' key of its application environment. Each entry is a map
%%% that must contain an `id' atom. Optional keys with their fallback
%%% defaults are:
%%%
%%% ```
%%% #{ id                    => atom(),
%%%    initial_remaining     => non_neg_integer(),
%%%    max_queue_length      => non_neg_integer(),
%%%    concurrency_window_ms => non_neg_integer() }
%%% '''
%%%
%%% If `groups' is unset, the application falls back to the built-in
%%% list defined in `arweave_client_throttling.hrl' (currently:
%%% `general' and `data_sync_record').
%%% @end
%%%===================================================================
-module(arweave_client_throttling_config).
-vsn(1).
-export([
    get_groups/0,
    get_group/1,
    get_value/2,
    default_groups/0,
    normalize_group/1
]).

-include("arweave_client_throttling.hrl").

%% @doc Return the configured list of group specs, normalized so that
%% every spec is a map containing every supported key.
-spec get_groups() -> [map()].
get_groups() ->
    Raw = case application:get_env(arweave_client_throttling, groups) of
              {ok, L} when is_list(L) -> L;
              _ -> default_groups()
          end,
    [normalize_group(G) || G <- Raw].

%% @doc Return the normalized spec for a single group, or
%% `{error, not_found}'.
-spec get_group(atom()) -> {ok, map()} | {error, not_found}.
get_group(Id) when is_atom(Id) ->
    case lists:search(fun(#{id := X}) -> X =:= Id end, get_groups()) of
        {value, G} -> {ok, G};
        false -> {error, not_found}
    end.

%% @doc Return a single value from a group spec.
-spec get_value(atom(), atom()) -> {ok, term()} | {error, term()}.
get_value(Id, Key) ->
    case get_group(Id) of
        {ok, Group} ->
            case maps:find(Key, Group) of
                {ok, V} -> {ok, V};
                error -> {error, {key_not_found, Id, Key}}
            end;
        Error -> Error
    end.

%% @doc Return the built-in default list of group specs.
-spec default_groups() -> [map()].
default_groups() ->
    [
     #{id => general},
     #{id => data_sync_record}
    ].

%% @doc Return a normalized version of `Group', filling in all
%% optional keys with their defaults.
-spec normalize_group(map()) -> map().
normalize_group(#{id := Id} = Group) when is_atom(Id) ->
    maps:merge(defaults(Id), Group).

defaults(Id) ->
    #{
        id => Id,
        initial_remaining =>
            ?ARWEAVE_CLIENT_THROTTLING_DEFAULT_INITIAL_REMAINING,
        max_queue_length =>
            ?ARWEAVE_CLIENT_THROTTLING_DEFAULT_MAX_QUEUE_LENGTH,
        concurrency_window_ms =>
            ?ARWEAVE_CLIENT_THROTTLING_DEFAULT_CONCURRENCY_WINDOW_MS
    }.
