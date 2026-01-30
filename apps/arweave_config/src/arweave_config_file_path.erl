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
%%% @doc Arweave Configuration File Checker.
%%%
%%% This module contains the code to check configuration files used by
%%% arweave and it is used by all `arweave_config_file_*' module. The
%%% goal is to have understandable and flexible rules easy to reuse in
%%% other part of the code.
%%%
%%% At the end of the pipeline, the path must have been fully checked
%%% and its content stored in `data' map key.
%%%
%%% Here the check steps:
%%%
%%% 1. check if the path has the right type (binary).
%%%
%%% 2. check if the path is absolute (or convert it).
%%%
%%% 3. check if the file exists.
%%%
%%% 4. check if the file is readable.
%%%
%%% 5. check if the file extension is correct.
%%%
%%% 6. read the content of the file.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_file_path).
-compile(warnings_as_errors).
-export([
	check/1,
	init/1,
	check_path_type/1,
	check_path/1,
	check_relative_path/1,
	check_file_mode/1,
	extract_directory/1,
	extract_extension/1,
	read_file/1
]).
-include_lib("kernel/include/file.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
check(Path) ->
	arweave_config_fsm:init(
		?MODULE,
		init,
		#{
			path => Path
		}
	).

%%--------------------------------------------------------------------
%% @private
%% @doc init the file checker.
%% @end
%%--------------------------------------------------------------------
-spec init(State) -> Return when
	State :: map(),
	Return :: term().

init(State) ->
	case file:get_cwd() of
		{ok, Cwd} ->
			NewState = State#{
				cwd => Cwd
			},
			{next, check_path_type, NewState};
		_ ->
			{error, "can't find current working directory"}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Check the Erlang path type. To avoid type confusion, all
%% configuration file path are encoded using binary type and not list.
%% @end
%%--------------------------------------------------------------------
check_path_type(State = #{ path := Path }) when is_list(Path) ->
	NewState = State#{
		path => list_to_binary(Path)
	},
	{next, check_path, NewState};
check_path_type(State = #{ path := Path }) when is_binary(Path) ->
	{next, check_path, State};
check_path_type(_State) ->
	{error, "bad path type"}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Check if a path is relative or absolute. Relative paths should
%% not be allowed, only absolute path should be used.
%% @end
%%--------------------------------------------------------------------
check_path(State = #{ path := Path }) ->
	case filename:pathtype(Path) of
		relative ->
			{next, check_relative_path, State};
		absolute ->
			{next, check_file_mode, State}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Check if the relative file path is safe (without "../"). We
%% don't want the application to load unwanted files, this is a first
%% protection against potential file injections.
%% @end
%%--------------------------------------------------------------------
check_relative_path(State = #{ path := Path, cwd := Cwd }) ->
	case filelib:safe_relative_path(Path, Cwd) of
		unsafe ->
			{error, "unsafe path"};
		_ ->
			AbsolutePath = filename:absname(Path),
			NewState = State#{
				origin_path => Path,
				path => AbsolutePath
			},
			{next, check_file_mode, NewState}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Check file mode. A file must be readable at least.
%% @end
%%--------------------------------------------------------------------
check_file_mode(State = #{ path := Path }) ->
	case file:read_file_info(Path) of
		{ok, #file_info{
			type = regular,
			access = read
			}
		} ->
			{next, extract_extension, State};
		{ok, #file_info{
			type = regular,
			access = read_write
			}
		} ->
			{next, extract_extension, State};
		_Else ->
			{error, "bad path"}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Extract the file extension and store it in `file_extension'.
%% @end
%%--------------------------------------------------------------------
extract_extension(State = #{ path := Path }) ->
	Extension = filename:extension(Path),
	NewState = State#{
		file_extension => Extension
	},
	{next, extract_directory, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Extract the directory where the file is stored in
%% `file_directory'.
%% @end
%%--------------------------------------------------------------------
extract_directory(State = #{ path := Path }) ->
	Dir = filename:dirname(Path),
	NewState = State#{
		file_directory => Dir
	},
	{next, read_file, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc read file content.
%% @end
%%--------------------------------------------------------------------
read_file(State = #{ path := Path }) ->
	% At this step, the application must be able to read the file,
	% except if a race condition appears and the file removed or
	% ownership/mode are changed.
	{ok, Data} = file:read_file(Path),
	{ok, Data, State}.
