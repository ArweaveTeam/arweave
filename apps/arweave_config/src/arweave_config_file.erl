%%% @doc Config file parser. Validates the path, dispatches to the
%%% format parser matching the file extension, and returns the parsed
%%% per-leaf option map plus the canonical absolute path.
%%%
%%% Pure module: no process, no caching. Each call re-reads and
%%% re-parses the file.
-module(arweave_config_file).
-compile(warnings_as_errors).
-export([parse/1, parse/2]).
-include_lib("kernel/include/file.hrl").

%% @doc Parser module for each supported file extension.
-spec parsers() -> #{ binary() => atom() }.
parsers() ->
    #{
        <<".json">> => arweave_config_format_json,
        <<".yaml">> => arweave_config_format_yaml
    }.

%% @doc Parse a config file at `Path`.
%% @see parse/2
-spec parse(Path) -> Return when
    Path :: binary() | string(),
    Return :: {ok, {Path, map()}} | {error, term()}.
parse(Path) ->
    parse(Path, #{}).

%% @doc Parse a config file. Validates the file, picks a parser by
%% extension, and returns the parsed config along with the canonical
%% absolute path.
-spec parse(Path, Opts) -> Return when
    Path :: binary() | string(),
    Opts :: map(),
    Return :: {ok, {Path, map()}} | {error, term()}.
parse(Path, _Opts) ->
    case check_path(Path) of
        {ok, Data, #{path := AbsPath, file_extension := Extension}} ->
            case maps:get(Extension, parsers(), undefined) of
                undefined ->
                    {error, "unsupported file"};
                Parser when is_atom(Parser) ->
                    case Parser:parse(Data) of
                        {ok, Config} -> {ok, {AbsPath, Config}};
                        Else -> Else
                    end;
                _ ->
                    {error, "unsupported extension or parser"}
            end;
        Else ->
            Else
    end.

%% Walk the validation pipeline: normalize to binary, resolve relative
%% paths (rejecting `../` escapes), require a regular readable file,
%% extract extension + directory, and read the contents.
check_path(Path) ->
    case file:get_cwd() of
        {ok, Cwd} ->
            check_path_type(#{path => Path, cwd => Cwd});
        _ ->
            {error, "can't find current working directory"}
    end.

check_path_type(State = #{path := Path}) when is_list(Path) ->
    check_pathtype(State#{path => list_to_binary(Path)});
check_path_type(State = #{path := Path}) when is_binary(Path) ->
    check_pathtype(State);
check_path_type(_State) ->
    {error, "bad path type"}.

check_pathtype(State = #{path := Path}) ->
    case filename:pathtype(Path) of
        relative -> check_relative_path(State);
        absolute -> check_file_mode(State)
    end.

check_relative_path(State = #{path := Path, cwd := Cwd}) ->
    case filelib:safe_relative_path(Path, Cwd) of
        unsafe ->
            {error, "unsafe path"};
        _ ->
            check_file_mode(State#{
                origin_path => Path,
                path => filename:absname(Path)
            })
    end.

check_file_mode(State = #{path := Path}) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = regular, access = read}} -> extract_extension(State);
        {ok, #file_info{type = regular, access = read_write}} -> extract_extension(State);
        _Else -> {error, "bad path"}
    end.

extract_extension(State = #{path := Path}) ->
    extract_directory(State#{file_extension => filename:extension(Path)}).

extract_directory(State = #{path := Path}) ->
    read_file(State#{file_directory => filename:dirname(Path)}).

%% A race (file removed / permissions changed since `check_file_mode`)
%% surfaces here as a badmatch — fine for a startup-time read.
read_file(State = #{path := Path}) ->
    {ok, Data} = file:read_file(Path),
    {ok, Data, State}.
