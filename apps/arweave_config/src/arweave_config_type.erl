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
-compile(warnings_as_errors).
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
	base64url/1,
	logging_template/1
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
-spec integer(Integer) -> Return when
	Integer :: list() | binary() | integer(),
	Return :: {ok, integer()} | {error, term()}.

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
-spec pos_integer(Integer) -> Return when
	Integer :: list() | binary() | pos_integer(),
	Return :: {ok, pos_integer()} | {error, term()}.

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
-spec ipv4(IPv4) -> Return when
	IPv4 :: inet:ip4_address() | binary() | list(),
	Return :: {ok, list()} | {error, term()}.

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
-spec file(File) -> Return when
	File :: binary() | list(),
	Return :: {ok, binary()} | {error, term()}.

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
-spec tcp_port(Port) -> Return when
	Port :: pos_integer(),
	Return :: {ok, pos_integer()} | {error, term()}.

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
%% @doc a string type.
%% @todo to be defined correctly.
%% @end
%%--------------------------------------------------------------------
-spec string(String) -> Return when
	String :: list(),
	Return :: {ok, list()} | {error, term()}.

string(String) -> string(String, String).
string([], String) -> {ok, String};
string([H|T], String) when is_integer(H) -> string(T, String);
string(_, String) -> {error, String}.

%%--------------------------------------------------------------------
%% @doc check base64 type.
%% @end
%%--------------------------------------------------------------------
-spec base64(String) -> Return when
	String :: binary() | list(),
	Return :: {ok, binary()} | {error, term()}.

base64(List) when is_list(List) ->
	base64(list_to_binary(List));
base64(Binary) ->
	try {ok, base64:decode(Binary)}
	catch _:_ -> {error, Binary}
	end.

%%--------------------------------------------------------------------
%% @doc check base64url
%% @end
%%--------------------------------------------------------------------
-spec base64url(String) -> Return when
	String :: binary() | list(),
	Return :: {ok, binary()} | {error, term()}.

base64url(List) when is_list(List) ->
	base64url(list_to_binary(List));
base64url(Binary) ->
	try {ok, b64fast:decode(Binary)}
	catch _:_ -> {error, Binary}
	end.

%%--------------------------------------------------------------------
%% @doc Check, parse and convert a logging template from custom
%% parser.
%%
%% The rules are strict, only tab and space as separator, only ASCII
%% printable chars as word, only a limited list of chars for
%% existing atoms (`[a-zA-Z_]'). An atom is a word starting with a
%% null char and '%' symbol. All templates are terminated with "\n".
%%
%% @see logger_formatter:template/0
%%
%% == Examples ==
%%
%% ```
%% {ok, ["test", "\n"]} = logging_template("test").
%% {ok, [test, "\n"]} = logging_template("%test").
%% {ok, ["message:", msg, "\n"]} = logging_template("message: %msg").
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec logging_template(String) -> Return when
	String :: binary() | list(),
	Return :: {ok, [atom()|list()]} | {error, term()}.

logging_template(List) when is_list(List) ->
	logging_template_parse(list_to_binary(List));
logging_template(Binary) when is_binary(Binary) ->
	logging_template_parse(Binary).

logging_template_parse(Binary) ->
	logging_template_tokenizer(Binary, []).

logging_template_tokenizer(<<>>, Buffer) ->
	logging_template_parser(Buffer);
logging_template_tokenizer(<<Char, Rest/binary>>, Buffer)
	when Char =:= $ ; Char =:= $\t ->
		NewBuffer = [{null, Char}|Buffer],
		logging_template_tokenizer(Rest, NewBuffer);
logging_template_tokenizer(<<$%, Rest/binary>>, Buffer) ->
	case logging_template_token_atom(Rest) of
		{ok, Atom, NewRest} ->
			NewBuffer = [{atom, Atom}|Buffer],
			logging_template_tokenizer(NewRest, NewBuffer);
		Else ->
			Else
	end;
logging_template_tokenizer(Bin, Buffer) when is_binary(Bin) ->
	case logging_template_token_word(Bin) of
		{ok, Word, NewRest} ->
			logging_template_token_word(Bin),
			NewBuffer = [{word, Word}|Buffer],
			logging_template_tokenizer(NewRest, NewBuffer);
		Else ->
			Else
	end.

logging_template_token_atom(Binary) ->
	logging_template_token_atom(Binary, <<>>).
logging_template_token_atom(<<>>, Buffer) ->
	{ok, Buffer, <<>>};
logging_template_token_atom(Rest = <<Char, _/binary>>, Buffer)
	when Char =:= $ ; Char =:= $\t ->
		{ok, Buffer, Rest};
logging_template_token_atom(<<Char, Rest/binary>>, Buffer)
	when Char >= $a, Char =< $z;
	     Char >= $A, Char =< $Z;
	     Char >= $0, Char =< $9;
	     Char =:= $_ ->
		logging_template_token_atom(Rest, <<Buffer/binary, Char>>);
logging_template_token_atom(<<Char, _/binary>>, _Buffer) ->
	{error, {atom, Char}}.

logging_template_token_word(Binary) ->
	logging_template_token_word(Binary, <<>>).
logging_template_token_word(<<>>, Buffer) ->
	{ok, Buffer, <<>>};
logging_template_token_word(Rest= <<Char, _/binary>>, Buffer)
	when Char =:= $ ; Char =:= $\t ->
		{ok, Buffer, Rest};
logging_template_token_word(<<Char, Rest/binary>>, Buffer)
	when Char >= 21, Char =< 126 ->
		logging_template_token_word(Rest, <<Buffer/binary, Char>>);
logging_template_token_word(<<Char, _/binary>>, _Buffer) ->
	{error, {word, Char}}.

logging_template_parser(Tokens) ->
	logging_template_parser(Tokens, []).
logging_template_parser([], Buffer) ->
	{ok, Buffer ++ ["\n"]};
logging_template_parser([{null, Null}|Rest], Buffer) ->
	NewBuffer = [[Null]|Buffer],
	logging_template_parser(Rest, NewBuffer);
logging_template_parser([{atom, Atom}|Rest], Buffer) ->
	try
		Result = binary_to_existing_atom(Atom),
		NewBuffer = [Result|Buffer],
		logging_template_parser(Rest, NewBuffer)
	catch
		_:_ ->
			{error, {atom, Atom}}
	end;
logging_template_parser([{word, Word}|Rest], Buffer) ->
	NewBuffer = [binary_to_list(Word)|Buffer],
	logging_template_parser(Rest, NewBuffer).

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
