-module(av_csv).
-export([parse/1, parse/2, parse_file/1, parse_file/2]).

%%% A Comma Seperated Value reader and parser.

%% Takes a string in CSV format, returning a list of rows of data.
parse(Str) -> parse(Str, $,).
parse(Str, SepChar) ->
	lists:map(
		fun(Row) -> string:tokens(Row, [SepChar]) end,
		string:tokens(Str, "\n")
	).

%% Takes a filename and returns a parsed list of CSV rows.
parse_file(File) -> parse_file(File, $,).
parse_file(File, SepChar) ->
	{ok, Bin} = file:read_file(File),
	parse(binary_to_list(Bin), SepChar).
