%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @copyright 2025 (c) Arweave
%%% @doc Arweave Configuration Type Definition.
%%% @end
%%%===================================================================
-module(arweave_config_type).
-export([
	none/1,
	any/1,
	boolean/1,
	integer/1,
	pos_integer/1,
	ipv4/1,
	file/1,
	tcp_port/1,
	path/1,
	atom/1,
	string/1,
	base64/1,
	base64url/1

]).
-include_lib("kernel/include/file.hrl").

%%--------------------------------------------------------------------
%% @doc always returns an error.
%% @end
%%--------------------------------------------------------------------
-spec none(V) -> {error, V}.

none(V) -> {error, V}.

%%--------------------------------------------------------------------
%% @doc always returns the value.
%% @end
%%--------------------------------------------------------------------
-spec any(V) -> {ok, V}.

any(V) -> {ok, V}.

%%--------------------------------------------------------------------
%% @doc check if the data is an atom and convert list/binary to
%% existing atoms.
%% @end
%%--------------------------------------------------------------------
-spec atom(Input) -> Return when
	Input :: string() | binary() | atom(),
	Return :: {ok, atom()} | {error, Input}.

atom(List) when is_list(List) ->
	try {ok, list_to_existing_atom(List)}
	catch _:_ -> {error, List}
	end;
atom(Binary) when is_binary(Binary) ->
	try {ok, binary_to_existing_atom(Binary)}
	catch _:_ -> {error, Binary}
	end;
atom(V) when is_atom(V) -> {ok, V};
atom(V) -> {error, V}.

%%--------------------------------------------------------------------
%% @doc check booleans from binary, list, integer and atoms.
%% @end
%%--------------------------------------------------------------------
-spec boolean(Input) -> Return when
	Input :: string() | binary() | boolean(),
	Return :: {ok, boolean()} | {error, Input}.

boolean(<<"true">>) -> {ok, true};
boolean(<<"false">>) -> {ok, false};
boolean("true") -> {ok, true};
boolean("false") -> {ok, false};
boolean(true) -> {ok, true};
boolean(false) -> {ok, false};
boolean(V) -> {error, V}.

%%--------------------------------------------------------------------
%% @doc check integers.
%% @end
%%--------------------------------------------------------------------
integer(List) when is_list(List) ->
	try integer(list_to_integer(List))
	catch _:_ -> {error, List} end;
integer(Binary) when is_binary(Binary) ->
	try integer(binary_to_integer(Binary))
	catch _:_ -> {error, Binary} end;
integer(Integer) when is_integer(Integer) ->
	{ok, Integer};
integer(V) ->
	{error, V}.

%%--------------------------------------------------------------------
%% @doc check positive integers.
%% @end
%%--------------------------------------------------------------------
pos_integer(Data) ->
	case integer(Data) of
		{ok, Integer} when Integer >= 0 ->
			{ok, Integer};
		_Else ->
			{error, Data}
	end.

%%--------------------------------------------------------------------
%% @doc check ipv4 addresses.
%% @end
%%--------------------------------------------------------------------
ipv4(Tuple = {_, _, _, _}) ->
	case inet:is_ipv4_address(Tuple) of
		true ->
			ipv4(inet:ntoa(Tuple));
		false ->
			{error, Tuple}
	end;
ipv4(Binary) when is_binary(Binary) ->
	ipv4(binary_to_list(Binary));
ipv4(List) when is_list(List) ->
	case inet:parse_strict_address(List, inet) of
		{ok, _} ->
			{ok, list_to_binary(List)};
		_Elsewise ->
			{error, List}
	end;
ipv4(Elsewise) ->
	{error, Elsewise}.

%%--------------------------------------------------------------------
%% @doc Defines file type.
%% @todo if an unix socket path length is > 108, it will fail, needs
%% to be fixed.
%% @end
%%--------------------------------------------------------------------
file(List) when is_list(List) ->
	file(list_to_binary(List));
file(Binary) when is_binary(Binary) ->
	case filename:pathtype(Binary) of
		absolute ->
			file2(Binary);
		relative ->
			{ok, Cwd} = file:get_cwd(),
			Absolute = filename:join(Cwd, Binary),
			file2(Absolute)
	end;
file(Path) ->
	type_error(
		file,
		<<"unsupported format">>,
		#{ path => Path }
	 ).

% check if the directory is present, arweave_config should not
% be in charge of creating it.
file2(Path) ->
	Split = filename:split(Path),
	[_Filename|Reverse] = lists:reverse(Split),
	Directory = filename:join(lists:reverse(Reverse)),
	case filelib:is_dir(Directory) of
		true ->
			file3(Path, Directory);
		false ->
			type_error(
				file,
				<<"directory not found">>,
				#{
					path => Path,
					directory => Directory
				 }
			)
	end.

% check if the directory has a read/write access.
file3(Path, Directory) ->
	Split = filename:split(Path),
	[_Filename|_] = lists:reverse(Split),
	case file:read_file_info(Directory) of
		{ok, #file_info{access = read_write }} ->
			file4(Path);
		{error, Reason} ->
			type_error(
				file,
				Reason,
				#{ path => Path }
			)
	end.

% convert a list into path. It should not be the case there, but it's
% to avoid having different type format.
file4(Path) when is_list(Path) ->
	{ok, list_to_binary(Path)};
file4(Path) ->
	{ok, Path}.

%%--------------------------------------------------------------------
%% @doc check tcp port.
%% @end
%%--------------------------------------------------------------------
tcp_port(Binary) when is_binary(Binary) ->
	tcp_port(binary_to_integer(Binary));
tcp_port(List) when is_list(List) ->
	tcp_port(list_to_integer(List));
tcp_port(Integer) when is_integer(Integer) ->
	case Integer of
		_ when Integer >= 0, Integer =< 65535 ->
			{ok, Integer};
		_ ->
			{error, Integer}
	end.

%%--------------------------------------------------------------------
%% @doc check unix path.
%% @end
%%--------------------------------------------------------------------
path(List) when is_list(List) ->
	path(list_to_binary(List));
path(Binary) when is_binary(Binary) ->
	case filename:validate(Binary) of
		true ->
			path_relative(Binary);
		false ->
			{error, Binary}
	end.

path_relative(Path) ->
	case filename:pathtype(Path) of
		relative ->
			{ok, filename:absname(Path)};
		absolute ->
			{ok, Path}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
string(String) -> string(String, String).
string([], String) -> {ok, String};
string([H|T], String) when is_integer(H) -> string(T, String);
string(_, String) -> {error, String}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
base64(List) when is_list(List) ->
	base64(list_to_binary(List));
base64(Binary) ->
	try {ok, base64:decode(Binary)}
	catch _:_ -> {error, Binary}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
base64url(List) when is_list(List) ->
	base64url(list_to_binary(List));
base64url(Binary) ->
	try {ok, b64fast:decode(Binary)}
	catch _:_ -> {error, Binary}
	end.

%%--------------------------------------------------------------------
%% common format for all errors
%%--------------------------------------------------------------------
type_error(Name, Reason, Data) ->
	{error, #{
			status => error,
			message => #{
				type => Name,
				reason => Reason,
				data => Data
			}
		 }
	}.
