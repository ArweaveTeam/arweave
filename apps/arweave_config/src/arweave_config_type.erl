%%%===================================================================
%%% @doc Arweave Configuration Type Definition.
%%% @end
%%%===================================================================
-module(arweave_config_type).
-compile(export_all).
-include_lib("kernel/include/file.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
boolean(<<"true">>) -> {ok, true};
boolean(<<"false">>) -> {ok, false};
boolean("true") -> {ok, true};
boolean("false") -> {ok, false};
boolean(0) -> {ok, false};
boolean(1) -> {ok, true};
boolean(V) -> {error, V}.

%%--------------------------------------------------------------------
%% @doc
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
%% @doc
%% @end
%%--------------------------------------------------------------------
pos_integer(Data) ->
	case integer(Data) of
		{ok, Integer} when Integer >= 0 ->
			{ok, Integer};
		Elsewise ->
			{error, Data}
	end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
ipv4(Binary) when is_binary(Binary) ->
	ipv4(binary_to_list(Binary));
ipv4(List) when is_list(List) ->
	case inet:parse_strict_address(List, inet) of
		{ok, _} ->
			{ok, list_to_binary(List)};
		_Elsewise ->
			{error, List}
	end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
ipv6(Binary) when is_binary(Binary) ->
	ipv6(binary_to_list(Binary));
ipv6(List) when is_list(List) ->
	case inet:parse_strict_address(List, inet6) of
		{ok, _} ->
			{ok, list_to_binary(List)};
		_Elsewise ->
			{error, List}
	end.

%%--------------------------------------------------------------------
%% @doc Defines unix_sock type. An UNIX socket should have an absolute
%% path, with the correct right. If not, it should fail.
%% @TODO finish the implementation.
%% success steps:
%% 1. check if the filename does not exist
%% 2. check if the filename end with ".sock"
%% 3. check if the directory is read_write
%% 4. check if the owner or group are current user
%% @end
%%--------------------------------------------------------------------
unix_sock(Binary) when is_binary(Binary) ->
	case filename:pathtype(Binary) of
		absolute ->
			unix_sock2(Binary);
		_ ->
			{error, Binary}
	end.

unix_sock2(Path) ->
	Split = filename:split(Path),
	[Filename|Reverse] = lists:reverse(Split),
	Directory = filename:join(lists:reverse(Reverse)),
	case filelib:is_dir(Directory) of
		true ->
			unix_sock3(Path);
		false ->
			{error, Path}
	end.

unix_sock3(Path) ->
	Split = filename:split(Path),
	[Filename|_] = lists:reverse(Split),
	case file:read_info(Path) of
		{ok, #file_info{ access = read_write }} ->
			{ok, Path};
		_Elsewise ->
			{error, Path}
	end.

%%--------------------------------------------------------------------
%% @doc
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
%% @doc
%% @end
%%--------------------------------------------------------------------
path(List) when is_list(List) ->
	path(list_to_binary(List));
path(Binary) when is_binary(Binary) ->
	case filename:validate(Binary) of
		true ->
			{ok, Binary};
		false ->
			{error, Binary}
	end.
