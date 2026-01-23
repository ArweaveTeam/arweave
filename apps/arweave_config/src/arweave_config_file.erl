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
%%% @doc Arweave Configure File Interface.
%%%
%%% This process is in charge of managing the configuration files. All
%%% configuration files are stored in memory with their parsed
%%% content.
%%%
%%% == Usage ==
%%%
%%% ```
%%% % add a new path, if the path/file is valid, the parsed version is
%%% % returned.
%%% {ok, ValidConfig1} = arweave_config_file:add(Path1).
%%% {ok, ValidConfig2} = arweave_config_file:add(Path2).
%%%
%%% % get the merged configuration, if more than one path is present
%%% % all of them will be merged by alphanumeric order
%%% {ok, Merged} = arweave_config_file:get().
%%%
%%% % returns the parsed configuration from the path
%%% {ok, ValidConfig1} = arweave_config_file:get_by_path(Path1).
%%%
%%% % returns the paths currently stored and merged.
%%% {ok, Paths} = arweave_config_file:get_paths().
%%%
%%% % reset the configuration, the last merged configuration is
%%% % returned to the caller
%%% {ok, Merged} = arweave_config_file:reset().
%%%
%%% % load the configuration into arweave_config
%%% ok = arweave_config_file:load().
%%% '''
%%%
%%% == TODO ==
%%%
%%% @todo create `check/0' function. It will ensure all files are still
%%% present and with the same values. If it's not the case, the files
%%% are reloaded and merged together. will be used with sighup.
%%%
%%% @todo create `delete/1' function. A configuration file can be removed
%%% from the store, in this case, the remaining files are merged
%%% together after the file has been removed.
%%%
%%% @todo create `get_by_format/1' function. It will return the
%%% configuration files by their format.
%%%
%%% @todo store the raw value and the parsed value in the store.
%%% Useful for debugging and analysis.
%%%
%%% @todo find a way to deal with a transition when a file is modified
%%% locally.
%%%
%%% @todo what to do if a configuration file can't be loaded?
%%%
%%% @todo should we store the parsed configuration files?
%%%
%%% @todo should we follow the files in case of modification
%%%       (e.g. inotify)?
%%%
%%% @todo add support for glob pattern
%%%
%%% @end
%%%===================================================================
-module(arweave_config_file).
-compile(warnings_as_errors).
-behavior(gen_server).
-export([
	start_link/0,
	add/1,
	get/0,
	get_by_path/1,
	get_paths/0,
	load/0,
	load/1,
	reset/0,
	parsers/0
]).
-export([
	parse/1,
	parse/2,
	check_path/1,
	identify_parser/1,
	parse_data/1
]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

%%--------------------------------------------------------------------
%% @doc start arweave_config_file process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc Returns the list of supported parsers based on the file
%% extension. If another extension needs to be supported, this parsers
%% list can be modified (ensure the test suite is working though).
%% @end
%%--------------------------------------------------------------------
-spec parsers() -> #{ binary() => atom() }.

parsers() ->
	#{
		<<".json">> => arweave_config_format_json,
		<<".yaml">> => arweave_config_format_yaml,
		<<".toml">> => arweave_config_format_toml,
		<<".ljson">> => arweave_config_format_legacy
	}.

%%--------------------------------------------------------------------
%% @doc Add a new configuration file path. The configuration must be
%% valid. The format of the file is identified using the postfix (e.g.
%% json, yaml, toml, ljson).
%% @end
%%--------------------------------------------------------------------
-spec add(Path) -> Return when
	Path :: string() | binary(),
	Return :: {ok, [map()]}.

add(Path) when is_list(Path) ->
	add(list_to_binary(Path));
add(Path) ->
	gen_server:call(?MODULE, {add, Path}, 1000).

%%--------------------------------------------------------------------
%% @doc Get the final merged configuration.
%% @end
%%--------------------------------------------------------------------
-spec get() -> Return when
	Return :: map().

get() ->
	gen_server:call(?MODULE, get, 1000).

%%--------------------------------------------------------------------
%% @doc Get the configuration file from a stored path.
%% @end
%%--------------------------------------------------------------------
-spec get_by_path(Path) -> Return when
	Path :: string() | binary(),
	Return :: {ok, {Timestamp, map()}} | {error, term()},
	Timestamp :: pos_integer().

get_by_path(Path) when is_list(Path) ->
	get_by_path(list_to_binary(Path));
get_by_path(Path) ->
	gen_server:call(?MODULE, {get, Path}, 1000).

%%--------------------------------------------------------------------
%% @doc Get the list of configuration file stored.
%% @end
%%--------------------------------------------------------------------
-spec get_paths() -> [binary()].

get_paths() ->
	gen_server:call(?MODULE, get_paths, 1000).

%%--------------------------------------------------------------------
%% @doc Load the merged configuration in arweave_config.
%% @end
%%--------------------------------------------------------------------
-spec load() -> Return when
	Return :: ok | timeout.

load() ->
	gen_server:call(?MODULE, load, 1000).

%%--------------------------------------------------------------------
%% @doc Load a specific stored configuration file in arweave_config.
%% @end
%%--------------------------------------------------------------------
-spec load(Path) -> Return when
	Path :: string() | binary(),
	Return :: {ok, map()}.

load(Path) when is_list(Path) ->
	load(list_to_binary(Path));
load(Path) ->
	gen_server:call(?MODULE, {load, Path}, 1000).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec reset() -> ok.

reset() ->
	gen_server:call(?MODULE, reset, 1000).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	Store = ets:new(?MODULE, [
		named_table,
		ordered_set,
		protected
	]),
	{ok, Store}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call({add, Path}, _From, Store) ->
	handle_add(Path, Store);
handle_call(get, _From, Store) ->
	handle_get(Store);
handle_call({get, Path}, _From, Store) ->
	handle_get_path(Path, Store);
handle_call(get_paths, _From, Store) ->
	handle_get_paths(Store);
handle_call(load, _From, Store) ->
	handle_load(Store);
handle_call({load, Path}, _From, Store) ->
	handle_load(Path, Store);
handle_call(reset, _From, Store) ->
	handle_reset(Store);
handle_call(_Msg, _From, State) ->
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_get(Store) ->
	case ets:lookup(?MODULE, merge) of
		[] ->
			{reply, #{}, Store};
		[{merge, Config}] ->
			{reply, Config, Store}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_get_paths(Store) ->
	Pattern = {{config, '$1'}, '_'},
	Guard = [],
	Format = ['$1'],
	Return = ets:select(?MODULE, [{Pattern, Guard, Format}]),
	{reply, Return, Store}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_get_path(Path, Store) ->
	Pattern = {{config, Path}, '$2'},
	Guard = [],
	Format = ['$2'],
	case ets:select(?MODULE, [{Pattern, Guard, Format}]) of
		[Config] ->
			{reply, {ok, Config}, Store};
		_ ->
			{reply, {error, not_found}, Store}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_load(Store) ->
	{reply, Config, _} = handle_get(Store),
	maps:map(fun (K, V) ->
		arweave_config:set(K, V)
	end, Config),
	{reply, ok, Store}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_load(Path, Store) ->
	case handle_get_path(Path, Store) of
		{reply, {ok, {_, Config}}, _} ->
			maps:map(fun (K, V) ->
				arweave_config:set(K, V)
			end, Config),
			{reply, ok, Store};
		Else ->
			Else
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_add(Path, Store) ->
	case parse(Path) of
		{ok, {ValidPath, Config}} ->
			Key = {config, ValidPath},
			Value = {erlang:system_time(), Config},
			ets:insert(?MODULE, {Key, Value}),
			handle_merge(Store);
		Else ->
			{reply, Else, Store}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_merge(Store) ->
	Merged = ets:foldl(fun
			({{config, _}, {_, Config}}, Acc) ->
				maps:merge(Acc, Config);
      			(_, Acc) ->
				Acc
		end,
  		#{},
		?MODULE
	),
	ets:insert(?MODULE, {merge, Merged}),
	{reply, {ok, Merged}, Store}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_reset(Store) ->
	ets:delete_all_objects(Store),
	{reply, ok, Store}.

%%--------------------------------------------------------------------
%% @doc parses a path.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Path) -> Return when
	Path :: binary() | string(),
	Return :: {ok, {Path, map()}} | {error, term()}.

parse(Path) ->
	parse(Path, #{}).

%%--------------------------------------------------------------------
%% @doc parses a path.
%% @end
%%--------------------------------------------------------------------
-spec parse(Path, Opts) -> Return when
	Path :: binary() | string(),
	Opts :: map(),
	Return :: {ok, {Path, map()}} | {error, term()}.

parse(Path, Opts) ->
	State = #{
		opts => Opts,
		path => Path
	},
	arweave_config_fsm:init(?MODULE, check_path, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc check the path used for the configuration file.
%% @end
%%--------------------------------------------------------------------
check_path(_State = #{ path := Path }) ->
	case arweave_config_file_path:check(Path) of
		{ok, Data, NewState} ->
			{next, identify_parser, NewState#{ data => Data }};
		Else ->
			Else
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc check the file extension (generated during the check), and
%% select the parser.
%% @end
%%--------------------------------------------------------------------
identify_parser(State = #{ file_extension := Extension }) ->
	Parsers = parsers(),
	case maps:get(Extension, Parsers, undefined) of
		undefined ->
			{error, "unsupported file"};
		Parser when is_atom(Parser) ->
			NewState = State#{
				parser => Parser
			},
			{next, parse_data, NewState};
		_ ->
			{error, "unsupported extension or parser"}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc
%% @end return the path (absolute) and the configuration (parsed).
%%--------------------------------------------------------------------
parse_data(_State = #{ path := Path, data := Data, parser := Parser }) ->
	case Parser:parse(Data) of
		{ok, Config} ->
			{ok, {Path, Config}};
		Else ->
			Else
	end.
