-module(av_utils).
-export([hex_to_binary/1, binary_to_hex/1]).
-export([unique/1, md5sum/1]).

%%% A module containing misc functions.

%% Delete repeated elements in a list.
unique(L) -> sets:to_list(sets:from_list(L)).

%% Convert a binary into a hex text string.
binary_to_hex(Bin) ->
	lists:flatten([io_lib:format("~2.16.0B", [X]) ||
		X <- binary_to_list(Bin)]).

%% Convert a hex string into a binary.
hex_to_binary(S) ->
	hex_to_binary(S, []).
hex_to_binary([], Acc) ->
	list_to_binary(lists:reverse(Acc));
hex_to_binary([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	hex_to_binary(T, [V | Acc]).

%% Calculate the MD5 sum for a file, returning it in binary format.
%% We should improve this later to only load 1mb chunks at a time.
%% We could even spawn 2 processes, one to read from the HD, and
%% the other to do the hashing. That might be much more efficient
%% for large files.
md5sum(FileName) when is_list(FileName) ->
	{ok, Bin} = file:read_file(FileName),
	md5sum(Bin);
md5sum(Bin) ->
	erlang:md5(Bin).
