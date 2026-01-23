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

	% @todo what to do if a configuration file can't be loaded?
	% @todo should we store the parsed configuration files?
	% @todo should we follow the files in case of modification
	%       (e.g. inotify)?
	% @todo add support for glob pattern

	% by default, arweave_config_file should return empty values,
	% if no configuration files are set
	{ok, #{}} = arweave_config_file:get(),
	{ok, []} = arweave_config_file:get_file(),

	% let load a configuration file using a path
	JsonFile = filename:join(DataDir, "config.json"),
	{ok, _} = arweave_config_file:add(JsonFile),

	% get the parsed configuration file
	{ok, _} = arweave_config_file:get(),

	% get information about the configuration file
	{ok, _} = arweave_config_file:get_files(),

	% load the configuration into arweave_config. We should be
	% able to read those values using arweave_config:get/1
	arweave_config_file:load(),

	% we can reset the state to start with a fresh process
	ok = arewave_config_file:reset(),

	% let load a toml file
	TomlFile = filename:join(DataDir, "config.toml"),
	{ok, _} = arweave_config_file:add(TomlFile),
	{ok, _} = arweave_config_file:get(),
	{ok, _} = arweave_config_file:get_files(),
	{ok, _} = arweave_config_file:load(),

	% let load a yaml file
	YamlFile = filename:join(DataDir, "config.yaml"),
	{ok, _} = arweave_config_file:add(YamlFile),
	{ok, _} = arweave_config_file:get(),
	{ok, _} = arweave_config_file:get_files(),
	{ok, _} = arweave_config_file:load(),
	ok = arweave_config_file:reset(),

	% let merge files together
	{ok, _} = arweave_config_file:add(JsonFile),
	{ok, _} = arweave_config_file:add(TomlFile),
	{ok, _} = arweave_config_file:add(YamlFile),

	% it should return the merged configuration file, where the
	% files are sorted in alphabetical order and loaded by their
	% full name.
	{ok, _} = arweave_config_file:get(),
	{ok, _} = arweave_config_file:load(),

	% it should return the list of files loaded in a list, sorted
	% in alphabetical order.
	{ok, _} = arweave_config_file:get_files(),

	% finally, reset the configuration
	ok = arweave_config_file:reset(),

	% try to load an invalid file
	InvalidFile = filename:join(DataDir, "invalid_file.json"),
	{error, _} = arweave_config_file:add(InvalidFile),

	{comment, "tested arweave config file worker"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test json file support.
%% @end
%%--------------------------------------------------------------------
json(Config) ->
	{error, _} = file_bad_format([{filename, "bad_format.json"}|Config]),
	{error, _} = file_bad_name([{filename, "bad_file_name.jsn"}|Config]),
	{ok, _} = file_empty([{filename, "empty.json"}|Config]),
	{error, _} = file_norights([{filename, "norights.json"}|Config]),
	{ok, _} = file_read([{filename, "read.json"}|Config]),
	{ok, _} = file_readwrite([{filename, "readwrite.json"}|Config]),
	{comment, "json file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test toml file support.
%% @end
%%--------------------------------------------------------------------
toml(Config) ->
	{error, _} = file_bad_format([{filename, "bad_format.toml"}|Config]),
	{error, _} = file_bad_name([{filename, "bad_file_name.tml"}|Config]),
	{ok, _} = file_empty([{filename, "empty.toml"}|Config]),
	{error, _} = file_norights([{filename, "norights.toml"}|Config]),
	{ok, _} = file_read([{filename, "read.toml"}|Config]),
	{ok, _} = file_readwrite([{filename, "readwrite.toml"}|Config]),
	{comment, "toml file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test yaml file support.
%% @end
%%--------------------------------------------------------------------
yaml(Config) ->
	{error, _} = file_bad_format([{filename, "bad_format.yaml"}|Config]),
	{error, _} = file_bad_name([{filename, "bad_file_name.yml"}|Config]),
	{ok, _} = file_empty([{filename, "empty.yaml"}|Config]),
	{error, _} = file_norights([{filename, "norights.yaml"}|Config]),
	{ok, _} = file_read([{filename, "read.yaml"}|Config]),
	{ok, _} = file_readwrite([{filename, "readwrite.yaml"}|Config]),
	{comment, "yaml file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc test legacy file support.
%% @end
%%--------------------------------------------------------------------
legacy(Config) ->
	{error, _} = file_bad_format([{filename, "bad_format.ljson"}|Config]),
	{error, _} = file_bad_name([{filename, "bad_file_name.lson"}|Config]),
	{ok, _} = file_empty([{filename, "empty.ljson"}|Config]),
	{error, _} = file_norights([{filename, "norights.ljson"}|Config]),
	{ok, _} = file_read([{filename, "read.ljson"}|Config]),
	{ok, _} = file_readwrite([{filename, "readwrite.ljson"}|Config]),
	{comment, "legacy file format tested"}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc create a regular file containing bad data.
%% @end
%%--------------------------------------------------------------------
file_bad_format(Config) ->
	Module = proplists:get_value(module, Config),
	PrivDir = proplists:get_value(priv_dir, Config),
	Filename = proplists:get_value(filename, Config),
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
	Filename = proplists:get_value(filename, Config),
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
	Filename = proplists:get_value(filename, Config),
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
	Filename = proplists:get_value(filename, Config),
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
	Filename = proplists:get_value(filename, Config),
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
	Filename = proplists:get_value(filename, Config),
	Path = filename:join(PrivDir, Filename),
	ct:pal(test, 1, "create file ~p", [Path]),
	file:write_file(Path, ""),
	file:change_mode(Path, 8#600),
	arweave_config_file:parse(Path).
