%%% @doc Arweave Configuration Bootstrap module.
%%%
%%% Drives the load sequence that populates `arweave_config` from the
%%% OS environment, a config file, and CLI args before the node
%%% transitions to runtime mode.
%%%
%%% Both current and legacy CLI dialects are supported. The dialect
%%% is determined by sniffing `Args`. A long-form CLI flag indicates
%%% the current dialect.
%%%
%%% Neither parser tolerates tokens from the other dialect, so we
%%% can't run both on the same arg list. Mixed-dialect arg lists
%%% aren't supported by either upstream parser.
%%%
%%% Pipeline:
%%%
%%%   1. Inspect `Args` once: determine dialect AND locate the
%%%      config_file directive (at most one source — env's
%%%      `AR_CONFIG_FILE`, current `--config_file`, or legacy
%%%      `config_file`).
%%%   2. Apply the config file (if any).
%%%   3. Apply the env map, overriding file values.
%%%   4. Apply the CLI args, overriding both file and env values.
%%%
-module(arweave_config_bootstrap).
-compile(warnings_as_errors).
-export([start/1]).

%% @doc Configure Arweave options from the OS environment, config file
%% (if any), and CLI args. The assembled state lives in the options
%% registry; callers read it back through `arweave_config:get/1`.
-spec start(Args) -> Return when
	Args :: [string() | binary()],
	Return :: ok | {error, term()}.
start(Args) ->
	Env = arweave_config_format_env:parse(),
	%% current or legacy
	Dialect = dialect(Env, Args),
	maybe
		{ok, ConfigFile} ?= find_config_file(Dialect, Args, Env),
		ok ?= apply_config_file(ConfigFile),
		ok ?= arweave_config:load(maps:remove([config_file], Env)),
		apply_cli(Dialect, Args)
	end.

%% Sniff `Args' to determine which CLI dialect is in play. The legacy
%% pipeline kicks in only when no `--'-prefixed token is present.
dialect(Env, _Args) when map_size(Env) > 0 ->
    current;
dialect(_Env, Args) ->
	case arweave_config_format_cli:has_long_flag(Args) of
		true -> current;
		false -> legacy
	end.

%% Locate at most one config_file across env and the dialect-
%% appropriate CLI parser. Returns the path (legacy paths tagged
%% `{legacy, Path}'), `none' when no source provides one, or
%% `{error, _}' when more than one source provides one (or a parser
%% reports a problem). Env's `AR_CONFIG_FILE' is always treated as a
%% current-format file regardless of CLI dialect.
find_config_file(Dialect, Args, Env) ->
	EnvConfigFile = arweave_config_format_env:find_config_file(Env),
	CLIConfigFile = cli_find_config_file(Dialect, Args),
	case {EnvConfigFile, CLIConfigFile} of
		{none, none} -> {ok, none};
		{{ok, Path}, none} -> {ok, Path};
		{none, {ok, Path}} -> {ok, Path};
		{{ok, _}, {ok, _}} -> {error, multiple_config_files};
		{{error, _} = Err, _} -> Err;
		{_, {error, _} = Err} -> Err
	end.

cli_find_config_file(current, Args) ->
	arweave_config_format_cli:find_config_file(Args);
cli_find_config_file(legacy, Args) ->
	case arweave_config_format_legacy_cli:find_config_file(Args) of
		{ok, Path} -> {ok, {legacy, Path}};
		Other -> Other
	end.

apply_config_file(none) -> ok;
apply_config_file({legacy, Path}) -> load_legacy_config_file(Path);
apply_config_file(Path) -> load_config_file(Path).

apply_cli(current, Args) ->
	case arweave_config_format_cli:parse(Args) of
		{ok, Map} -> arweave_config:load(Map);
		{error, _} = Err -> Err
	end;
apply_cli(legacy, Args) ->
	arweave_config_format_legacy_cli:parse(Args).

load_config_file(Path) ->
	case arweave_config_file:parse(Path) of
		{ok, {AbsPath, Config}} ->
			case arweave_config:load(Config) of
				ok ->
					arweave_config:set([config_file], AbsPath),
					ok;
				{error, _} = Err ->
					Err
			end;
		{error, _} = Err ->
			Err
	end.

load_legacy_config_file(Path) ->
	case arweave_config_format_legacy_json:parse_config_file(["config_file", Path]) of
		ok -> ok;
		{error, Reason} -> {error, Reason};
		{error, Reason, _} -> {error, Reason}
	end.

