%% @doc Returns a map of `OptionKey => Value` binaries built from
%% the OS environment variables (e.g. `AR_VDF_IS_PUBLIC_SERVER`).
%% Only Options that support being set via the environment are included.
-module(arweave_config_format_env).
-compile(warnings_as_errors).
-export([parse/0, find_config_file/1]).

-spec parse() -> #{list() => binary()}.
parse() ->
    Bindings = arweave_config_options_registry:get_environments(),
    lists:foldl(
        fun(E, Acc) ->
            case re:split(E, "=", [{parts, 2}, {return, list}]) of
                [K, V] ->
                    Key = list_to_binary(K),
                    case lists:keyfind(Key, 1, Bindings) of
                        {Key, OptionKey} -> Acc#{OptionKey => list_to_binary(V)};
                        false -> Acc
                    end;
                _ ->
                    Acc
            end
        end,
        #{},
        os:getenv()).

%% @doc Extract the `[config_file]' entry from a parsed env map, if
%% any. Matches the uniform `find_config_file' interface used by the
%% CLI / legacy-CLI parsers.
-spec find_config_file(map()) -> none | {ok, binary()}.
find_config_file(Env) ->
    case maps:get([config_file], Env, undefined) of
        undefined -> none;
        Path -> {ok, Path}
    end.
