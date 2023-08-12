%% Erlang OTP disksup copyright note:
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2018. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%	   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%%% @doc The server is a modified version of disksup from Erlang OTP - it periodically
%%% checks for available disk space and returns it in bytes (disksup only serves it in %).
%%% @end
-module(ar_disksup).
-behaviour(gen_server).

-export([start_link/0, get_disk_space_check_frequency/0, get_disk_data/0, pause/0, resume/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	timeout,
	os,
	diskdata = [],
	port,
	paused = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_disk_space_check_frequency() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.disk_space_check_frequency.

get_disk_data() ->
	gen_server:call(?MODULE, get_disk_data, infinity).

pause() ->
	gen_server:call(?MODULE, pause, infinity).

resume() ->
	gen_server:call(?MODULE, resume, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	process_flag(priority, low),
	OS = get_os(),
	Port =
		case OS of
			{unix, Flavor}
				when Flavor==sunos4;
					Flavor==solaris;
					Flavor==freebsd;
					Flavor==dragonfly;
					Flavor==darwin;
					Flavor==linux;
					Flavor==posix;
					Flavor==openbsd;
					Flavor==netbsd ->
				start_portprogram();
		{win32, _OSname} ->
			not_used;
		_ ->
			exit({unsupported_os, OS})
		end,
	%% Initiate the first check.
	self() ! timeout,
	{ok, #state{ port = Port, os = OS, timeout = get_disk_space_check_frequency() }}.

handle_call(get_disk_data, _From, State) ->
	{reply, State#state.diskdata, State};

handle_call(pause, _From, State) ->
	{reply, ok, State#state{ paused = true }};

handle_call(resume, _From, State) ->
	{reply, ok, State#state{ paused = false }}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(timeout, #state{ paused = true } = State) ->
	{ok, _Tref} = timer:send_after(State#state.timeout, timeout),
	{noreply, State};
handle_info(timeout, State) ->
	NewDiskData = check_disk_space(State#state.os, State#state.port),
	ensure_storage_modules_paths(),
	broadcast_disk_free(State#state.os, State#state.port),
	{ok, _Tref} = timer:send_after(State#state.timeout, timeout),
	{noreply, State#state{ diskdata = NewDiskData }};

handle_info({'EXIT', _Port, Reason}, State) ->
	{stop, {port_died, Reason}, State#state{ port = not_used }};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, State) ->
	case State#state.port of
		not_used ->
			ok;
		Port ->
			port_close(Port)
	end,
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_os() ->
	case os:type() of
		{unix, sunos} ->
			case os:version() of
				{5, _, _} ->
					{unix, solaris};
				{4, _, _} ->
					{unix, sunos4};
				V -> exit({unknown_os_version, V})
			end;
		OS ->
			OS
	end.

start_portprogram() ->
	open_port({spawn, "sh -s disksup 2>&1"}, [stream]).

my_cmd(Cmd0, Port) ->
	%% Insert a new line after the command, in case the command
	%% contains a comment character.
	Cmd = io_lib:format("(~s\n) </dev/null; echo  \"\^M\"\n", [Cmd0]),
	Port ! {self(), {command, [Cmd, 10]}},
	get_reply(Port, []).

get_reply(Port, O) ->
	receive
		{Port, {data, N}} ->
			case newline(N, O) of
				{ok, Str} -> Str;
				{more, Acc} -> get_reply(Port, Acc)
			end;
		{'EXIT', Port, Reason} ->
			exit({port_died, Reason})
	end.

newline([13 | _], B) -> {ok, lists:reverse(B)};
newline([H | T], B) -> newline(T, [H | B]);
newline([], B) -> {more, B}.

find_cmd(Cmd) ->
	os:find_executable(Cmd).

find_cmd(Cmd, Path) ->
	%% Try to find it at the specific location.
	case os:find_executable(Cmd, Path) of
		false ->
			find_cmd(Cmd);
		Found ->
			Found
	end.

%% We use as many absolute paths as possible below as there may be stale
%% NFS handles in the PATH which cause these commands to hang.
check_disk_space({win32, _}, not_used) ->
	Result = os_mon_sysinfo:get_disk_info(),
	check_disks_win32(Result);
check_disk_space({unix, solaris}, Port) ->
	Result = my_cmd("/usr/bin/df -lk", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, linux}, Port) ->
	Df = find_cmd("df", "/bin"),
	Result = my_cmd(Df ++ " -lk -x squashfs", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, posix}, Port) ->
	Result = my_cmd("df -k -P", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, dragonfly}, Port) ->
	Result = my_cmd("/bin/df -k -t ufs,hammer", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, freebsd}, Port) ->
	Result = my_cmd("/bin/df -k -l", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, openbsd}, Port) ->
	Result = my_cmd("/bin/df -k -l", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, netbsd}, Port) ->
	Result = my_cmd("/bin/df -k -t ffs", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, sunos4}, Port) ->
	Result = my_cmd("df", Port),
	check_disks_solaris(skip_to_eol(Result));
check_disk_space({unix, darwin}, Port) ->
	Result = my_cmd("/bin/df -i -k -t ufs,hfs,apfs", Port),
	check_disks_susv3(skip_to_eol(Result)).

disk_free_cmd({unix, darwin}, Df, DataDirPath, Port) ->
	 my_cmd(Df ++ " -Pa " ++ DataDirPath ++ "/", Port);
disk_free_cmd({unix, _}, Df, DataDirPath, Port) ->
	 my_cmd(Df ++ " -Pa -B1 " ++ DataDirPath ++ "/", Port).

%% doc: iterates trough storage modules
broadcast_disk_free({unix, _} = Os, Port) ->
	Df = find_cmd("df"),
	[DataDirPathData | StorageModulePaths] = get_storage_modules_paths(),
	{DataDirID, DataDirPath} = DataDirPathData,
	DataDirDfResult = disk_free_cmd(Os, Df, DataDirPath, Port),
	[DataDirFs, DataDirBytes, DataDirPercentage] = parse_df_2(DataDirDfResult),
	ar_events:send(disksup, {
		remaining_disk_space,
			DataDirID,
			true,
			DataDirPercentage,
			DataDirBytes
	}),
	HandleSmPath = fun({StoreID, StorageModulePath}) ->
		Result = disk_free_cmd(Os, Df, StorageModulePath, Port),
		[StorageModuleFs, Bytes, Percentage] = parse_df_2(Result),
		IsDataDirDrive = string:equal(DataDirFs, StorageModuleFs),
		ar_events:send(disksup, {
			remaining_disk_space,
			StoreID, IsDataDirDrive, Percentage, Bytes
		})
	end,
	lists:foreach(HandleSmPath, StorageModulePaths);
broadcast_disk_free(_, _) ->
	ar:console("~nWARNING: disk space checks are not supported on your platform. The node "
			"may stop working if it runs out of space.~n", []).

%% This code works for Linux and FreeBSD as well.
check_disks_solaris("") ->
	[];
check_disks_solaris("\n") ->
	[];
check_disks_solaris(Str) ->
	case parse_df(Str, posix) of
		{ok, {KB, CapKB, MntOn}, RestStr} ->
			[{MntOn, KB, CapKB} | check_disks_solaris(RestStr)];
		_Other ->
			check_disks_solaris(skip_to_eol(Str))
	end.

%% @private
%% @doc Predicate to take a word from the input string until a space or
%% a percent '%' sign (the Capacity field is followed by a %)
parse_df_is_not_space($ ) -> false;
parse_df_is_not_space($%) -> false;
parse_df_is_not_space(_) -> true.

%% @private
%% @doc Predicate to take spaces away from string. Stops on a non-space
parse_df_is_space($ ) -> true;
parse_df_is_space(_) -> false.

%% @private
%% @doc Predicate to consume remaining characters until end of line.
parse_df_is_not_eol($\r) -> false;
parse_df_is_not_eol($\n) -> false;
parse_df_is_not_eol(_)	 -> true.

%% @private
%% @doc Trims leading non-spaces (the word) from the string then trims spaces.
parse_df_skip_word(Input) ->
	Remaining = lists:dropwhile(fun parse_df_is_not_space/1, Input),
	lists:dropwhile(fun parse_df_is_space/1, Remaining).

%% @private
%% @doc Takes all non-spaces and then drops following spaces.
parse_df_take_word(Input) ->
	{Word, Remaining0} = lists:splitwith(fun parse_df_is_not_space/1, Input),
	Remaining1 = lists:dropwhile(fun parse_df_is_space/1, Remaining0),
	{Word, Remaining1}.

%% @private
%% @doc Takes all non-spaces and then drops the % after it and the spaces.
parse_df_take_word_percent(Input) ->
	{Word, Remaining0} = lists:splitwith(fun parse_df_is_not_space/1, Input),
	%% Drop the leading % or do nothing.
	Remaining1 =
		case Remaining0 of
			[$% | R1] -> R1;
			_ -> Remaining0 % Might be no % or empty list even.
		end,
	Remaining2 = lists:dropwhile(fun parse_df_is_space/1, Remaining1),
	{Word, Remaining2}.

%% @private
%% @doc Given a line of 'df' POSIX/SUSv3 output split it into fields:
%% a string (mounted device), 4 integers (kilobytes, used, available
%% and capacity), skip % sign, (optionally for susv3 can also skip IUsed, IFree
%% and ICap% fields) then take remaining characters as the mount path
-spec parse_df(string(), posix | susv3) ->
	{error, parse_df} | {ok, {integer(), integer(), list()}, string()}.
parse_df(Input0, Flavor) ->
	%% Format of Posix/Linux df output looks like Header + Lines
	%% Filesystem	  1024-blocks	  Used Available Capacity Mounted on
	%% udev				  2467108		 0	 2467108	   0% /dev
	Input1 = parse_df_skip_word(Input0), % Skip device path field.
	{KBStr, Input2} = parse_df_take_word(Input1), % Take KB field.
	Input3 = parse_df_skip_word(Input2), % Skip Used field.
	{AvailKBStr, Input4} = parse_df_take_word(Input3), % Take Avail field.
	{_, Input5} = parse_df_take_word_percent(Input4), % Skip Capacity% field.
	%% Format of OS X/SUSv3 df looks similar to POSIX but has 3 extra columns
	%% Filesystem 1024-blocks Used Available Capacity iused ifree %iused Mounted
	%% /dev/disk1	243949060 2380	86690680	65% 2029724 37555	 0%  /
	Input6 =
		case Flavor of
			posix -> Input5;
			susv3 -> % There are 3 extra integers we want to skip.
				Input5a = parse_df_skip_word(Input5), % Skip IUsed field.
				Input5b = parse_df_skip_word(Input5a), % Skip IFree field.
				%% Skip the value of ICap + '%' field.
				{_, Input5c} = parse_df_take_word_percent(Input5b),
				Input5c
		end,
	%% Path is the remaining string till end of line.
	{MountPath, Input7} = lists:splitwith(fun parse_df_is_not_eol/1, Input6),
	%% Trim the newlines.
	Remaining = lists:dropwhile(fun(X) -> not parse_df_is_not_eol(X) end, Input7),
	try
		KB = erlang:list_to_integer(KBStr),
		CapacityKB = erlang:list_to_integer(AvailKBStr),
		{ok, {KB, CapacityKB, MountPath}, Remaining}
	catch error:badarg ->
		{error, parse_df}
	end.

%% Parse per SUSv3 specification, notably recent OS X.
check_disks_susv3("") ->
	[];
check_disks_susv3("\n") ->
	[];
check_disks_susv3(Str) ->
	case parse_df(Str, susv3) of
		{ok, {KB, CapKB, MntOn}, RestStr} ->
			[{MntOn, KB, CapKB} | check_disks_susv3(RestStr)];
		_Other ->
			check_disks_susv3(skip_to_eol(Str))
	end.

check_disks_win32([]) ->
	[];
check_disks_win32([H|T]) ->
	case io_lib:fread("~s~s~d~d~d", H) of
		{ok, [Drive, "DRIVE_FIXED", BAvail, BTot, _TotFree], _RestStr} ->
			[{Drive, BTot div 1024, BAvail div 1024} | check_disks_win32(T)];
		{ok, _, _RestStr} ->
			check_disks_win32(T);
		_Other ->
			[]
	end.

skip_to_eol([]) ->
	[];
skip_to_eol([$\n | T]) ->
	T;
skip_to_eol([_ | T]) ->
	skip_to_eol(T).

get_storage_modules_paths() ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	SMDirs = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			{StoreID, filename:join([DataDir, "storage_modules", StoreID])}
		end,
		Config#config.storage_modules
	),
	[{"default", DataDir} | SMDirs].

ensure_storage_modules_paths() ->
	StoragePaths = get_storage_modules_paths(),
	EnsurePaths = fun({_, StorageModulePath}) ->
		filelib:ensure_dir(StorageModulePath ++ "/")
	end,
	lists:foreach(EnsurePaths, StoragePaths).

parse_df_2(Input) ->
	[DfHeader, DfInfo] = string:tokens(Input, "\n"),
	[_, BlocksInfo | _] = string:tokens(DfHeader, " \t"),
	BlocksNum = case string:tokens(BlocksInfo, "-") of
		 [Num, _] ->
			 erlang:list_to_integer(Num);
		 _->
			 1
  end,
	[Filesystem, Total, _, Available, _, _] = string:tokens(DfInfo, " \t"),
	BytesAvailable = erlang:list_to_integer(Available),
	TotalCapacity = erlang:list_to_integer(Total),
	PercentageAvailable = BytesAvailable / TotalCapacity,
	[Filesystem, BytesAvailable * BlocksNum, PercentageAvailable].
