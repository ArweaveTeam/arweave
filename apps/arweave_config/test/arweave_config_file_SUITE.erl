%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Config File Support Test Suite.
%%% @end
%%%===================================================================
-module(arweave_config_file_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	default/1,
	load/1,
	json/1,
	yaml/1,
	toml/1,
	legacy/1
]).
-include("arweave_config.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
description() -> {description, ""}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_suite(Config) -> Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_suite(_Config) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
	ct:pal(info, 1, "start arweave_config"),
	ok = arweave_config:start(),
	Config.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_config"),
	ok = arweave_config:stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
all() ->
	[
		default,
		load,
		json,
		toml,
		yaml,
		legacy
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
default(Config) ->
	DataDir = proplists:get_value(data_dir, Config),

	ct:pal(test, 1, "Check post-initialization value"),
	#{} = arweave_config_file:get(),
	[] = arweave_config_file:get_paths(),

	ct:pal(test, 1, "Check presence of non loaded file"),
	{error, not_found} =
		arweave_config_file:get_by_path("/tmp2/not_present.json"),

	ct:pal(test, 1, "check unsupported file"),
	{error, _} = arweave_config_file:add(
		filename:join(DataDir, "config_unsupported.xml")
	),

	{ok, JsonPath} = valid_format_test([
		{format, "json"},
		{filename, "config_valid.json"}
	|Config]),

	{ok, TomlPath} = valid_format_test([
		{format, "toml"},
		{filename, "config_valid.toml"}
	|Config]),

	{ok, YamlPath} = valid_format_test([
		{format, "yaml"},
		{filename, "config_valid.yaml"}
	|Config]),

	% @todo legacy
	% LegacyPath = filename:join(DataDir, "config_valid.ljson"),
	% format_test([
	% 	{format, "legacy"},
	% 	{filename, "config_valid.ljson"},
	% 	{path, LegacyPath}
	% |Config]),

	ct:pal(test, 1, "merge toml, json and yaml files together"),
	{ok, _} = arweave_config_file:add(JsonPath),
	{ok, _} = arweave_config_file:add(TomlPath),
	{ok, _} = arweave_config_file:add(YamlPath),

	ct:pal(test, 1, "check merged configuration"),
	_ = arweave_config_file:get(),

	% it should return the list of files loaded in a list, sorted
	% in alphabetical order.
	ct:pal(test, 1, "check if the paths have been added"),
	MergedPaths = arweave_config_file:get_paths(),
	true = search_path(JsonPath, MergedPaths),
	true = search_path(TomlPath, MergedPaths),
	true = search_path(YamlPath, MergedPaths),

	% finally, reset the configuration
	ct:pal(test, 1, "reset arweave_config_file state"),
	ok = arweave_config_file:reset(),

	{error, _InvalidJsonPath} = invalid_format_test([
		{format, "json"},
		{filename, "config_invalid.json"}
	|Config]),

	{error, _InvalidYamlPath} = invalid_format_test([
		{format, "yaml"},
		{filename, "config_invalid.yaml"}
	|Config]),

	{error, _InvalidTomlPath} = invalid_format_test([
		{format, "toml"},
		{filename, "config_invalid.toml"}
	|Config]),

	ok = arweave_config_file:load(),

	% check unsupported call, the process should not crash.
	ok = erlang:send(arweave_config_file, ok),
	ok = gen_server:cast(arweave_config_file, ok),
	ok = gen_server:call(arweave_config_file, unsupported, 1000),

	{comment, "tested arweave config file worker"}.


%%--------------------------------------------------------------------
%% @hidden
%% @doc test valid common pattern for all format.
%% @end
%%--------------------------------------------------------------------
valid_format_test(Config) ->
	DataDir = proplists:get_value(data_dir, Config),
	Format = proplists:get_value(format, Config),
	Filename = proplists:get_value(filename, Config),
	Path = filename:join(DataDir, Filename),

	ct:pal(test, 1, "~p: Add ~p file", [Format, Filename]),
	{ok, _} = arweave_config_file:add(Path),

	ct:pal(test, 1, "~p: check if the file has been added", [Format]),
	Paths = arweave_config_file:get_paths(),
	ct:pal(test, 1, "~p", [Paths]),
	true = search_path(Path, Paths),

	ct:pal(test, 1, "~p: check if the file has been parsed", [Format]),
	Merged = arweave_config_file:get(),
	true = 0 < map_size(Merged),

	ct:pal(test, 1, "~p: retrieve the configuration (~p)", [Format, Path]),
	{ok, {_Timestamp, _Config}} = arweave_config_file:get_by_path(Path),

	ct:pal(test, 1, "~p: load ~p", [Format, Path]),
	ok = arweave_config_file:load(Path),
	{ok, true} = arweave_config:get([debug]),

	ct:pal(test, 1, "~p: load merged configuration", [Format]),
	ok = arweave_config_file:load(),
	{ok, true} = arweave_config:get([debug]),

	ct:pal(test, 1, "reset arweave_config_file state"),
	ok = arweave_config_file:reset(),

	% return the full path
	{ok, Path}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test invalid common pattern for all format.
%% @end
%%--------------------------------------------------------------------
invalid_format_test(Config) ->
	DataDir = proplists:get_value(data_dir, Config),
	Format = proplists:get_value(format, Config),
	Filename = proplists:get_value(filename, Config),
	Path = filename:join(DataDir, Filename),
	ct:pal(test, 1, "~p: load invalid file ~p (~p)", [Format, Filename, Path]),
	{error, _} = arweave_config_file:add(Path),

	ct:pal(test, 1, "~p: ensure the file ~p was not loaded", [Format, Filename]),
	Paths = arweave_config_file:get_paths(),
	false = search_path(Path, Paths),

	ct:pal(test, 1, "reset arweave_config_file state"),
	ok = arweave_config_file:reset(),
	{error, Path}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load(_Config) ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test json file support.
%% @end
%%--------------------------------------------------------------------
json(Config) ->
	{error, _} = file_bad_name([{extension, ".jsn"}|Config]),
	file_checks([{extension, ".json"}|Config]),
	{comment, "json file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test toml file support.
%% @end
%%--------------------------------------------------------------------
toml(Config) ->
	{error, _} = file_bad_name([{extension, ".tml"}|Config]),
	file_checks([{extension, ".toml"}|Config]),
	{comment, "toml file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test yaml file support.
%% @end
%%--------------------------------------------------------------------
yaml(Config) ->
	{error, _} = file_bad_name([{extension, ".yml"}|Config]),
	file_checks([{extension, ".yaml"}|Config]),
	{comment, "yaml file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test legacy file support.
%% @end
%%--------------------------------------------------------------------
legacy(Config) ->
	{error, _} = file_bad_name([{extension, ".lson"}|Config]),
	file_checks([{extension, ".ljson"}|Config]),
	{comment, "legacy file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc check all common file test.
%% @end
%%--------------------------------------------------------------------
file_checks(Config) ->
	{error, _} = file_bad_format(Config),
	{ok, _} = file_empty(Config),
	{error, _} = file_norights(Config),
	{ok, _} = file_read(Config),
	{ok, _} = file_readwrite(Config),
	{error, _} = file_unsafe_path(Config),
	{ok, _} = file_relative_path(Config),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%% @doc create a regular file containing bad data.
%% @end
%%--------------------------------------------------------------------
file_bad_format(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["bad_format", Extension], ""),
	Data = proplists:get_value(data, Config, "test::data::bad"),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, Data),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc creates a regular file containing a bad name.
%% @end
%%--------------------------------------------------------------------
file_bad_name(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["bad_name", Extension], ""),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc creates an empty file.
%% @end
%%--------------------------------------------------------------------
file_empty(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["empty", Extension], ""),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc creates an empty file with no rights.
%% @end
%%--------------------------------------------------------------------
file_norights(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["norights", Extension], ""),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	file:change_mode(Path, 8#000),
	Return = arweave_config_file:parse(Path),
	file:change_mode(Path, 8#600),
	Return.

%%--------------------------------------------------------------------
%% @hidden
%% @doc creates an empty file in read-only.
%% @end
%%--------------------------------------------------------------------
file_read(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["read", Extension], ""),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	file:change_mode(Path, 8#400),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc creates an empty file in read/write.
%% @end
%%--------------------------------------------------------------------
file_readwrite(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["readwrite", Extension], ""),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	file:change_mode(Path, 8#600),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc check unsafe page (containing "../").
%% @end
%%--------------------------------------------------------------------
file_unsafe_path(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["unsafe_path", Extension], ""),
	Path = filename:join(["../..", Filename]),
	ct:pal(test, 1, "check unsafe path ~p", [Path]),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc check safe relative path.
%% @end
%%--------------------------------------------------------------------
file_relative_path(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Extension = proplists:get_value(extension, Config),
	Filename = string:join(["relative_path", Extension], ""),
	{ok, Cwd} = file:get_cwd(),
	Relative =
		case PrivDir -- Cwd of
			[$/|R] -> R;
			[$.,$/|R] -> R;
			E -> E
		end,
	Path = filename:join(["./", Relative, Filename]),
	ct:pal(test, 1, "check relative path ~p", [Path]),
	file:write_file(Path, ""),
	file:change_mode(Path, 8#644),
	arweave_config_file:parse(Path).

%%--------------------------------------------------------------------
%% @hidden
%% @doc search a term in a list.
%% @see lists:search/2
%% @end
%%--------------------------------------------------------------------
search_path(Path, List) when is_list(Path) ->
	search_path(list_to_binary(Path), List);
search_path(Path, List) ->
	Fun = fun
		(P) when P =:= Path -> true;
		(_) -> false
	end,
	case lists:search(Fun, List) of
		{value, _} -> true;
		_ -> false
	end.
