%%% @doc arweave_config_file:parse/1 test suite.
-module(arweave_config_file_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").

suite() ->
	[{timetrap, {seconds, 60}}].

all() ->
	[unsupported_extension, bad_format, empty_file,
	 unreadable_file, readable_file, unsafe_path, relative_path].

%%====================================================================
%% Test cases
%%====================================================================

unsupported_extension(Config) ->
	{error, _} = parse(Config, "bad.jsn", "", 8#600),
	{error, _} = parse(Config, "bad.yml", "", 8#600).

bad_format(Config) ->
	{error, _} = parse(Config, "bad.json", "test::data::bad", 8#600),
	{error, _} = parse(Config, "bad.yaml", "test::data::bad", 8#600).

empty_file(Config) ->
	{ok, _} = parse(Config, "empty.json", "", 8#600),
	{ok, _} = parse(Config, "empty.yaml", "", 8#600).

%% 8#000 — file exists but is unreadable
unreadable_file(Config) ->
	{error, _} = parse(Config, "norights.json", "", 8#000).

readable_file(Config) ->
	{ok, _} = parse(Config, "ro.json", "", 8#400),
	{ok, _} = parse(Config, "rw.yaml", "", 8#600).

unsafe_path(_Config) ->
	{error, _} = arweave_config_file:parse("../../escape.json").

relative_path(Config) ->
	PrivDir = ?config(priv_dir, Config),
	{ok, Cwd} = file:get_cwd(),
	Rel = filename:join(string:prefix(PrivDir, Cwd ++ "/"), "rel.json"),
	ok = file:write_file(filename:join(Cwd, Rel), ""),
	{ok, _} = arweave_config_file:parse("./" ++ Rel).

%%====================================================================
%% Helpers
%%====================================================================

parse(Config, Name, Content, Mode) ->
	Path = filename:join(?config(priv_dir, Config), Name),
	ok = file:write_file(Path, Content),
	ok = file:change_mode(Path, Mode),
	%% Restore readable mode after parsing so the artifact-upload
	%% step in CI can zip the priv_dir even when the parse throws.
	try
		arweave_config_file:parse(Path)
	after
		file:change_mode(Path, 8#600)
	end.
