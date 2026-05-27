%%% @doc Arweave Configuration YAML Format Support.
%%%
%%% @reference https://yaml.org/
%%% @reference https://github.com/yakaz/yamerl/
-module(arweave_config_format_yaml).
-compile(warnings_as_errors).
-export([
	parse/1,
	parse/2,
	encode/1,
	encode/2
]).

-spec parse(FileContents) -> Return when
	FileContents :: string() | binary(),
	Return :: {ok, map()} | {error, term()}.
parse(FileContents) ->
	parse(FileContents, #{}).

-spec parse(FileContents, Opts) -> Return when
	FileContents :: string() | binary(),
	Opts :: map(),
	Return :: {ok, map()} | {error, term()}.
parse(FileContents, _Opts) ->
	try
		Proplist = case yamerl:decode(FileContents) of
			[] -> [];
			[PL] -> PL;
			_ -> throw(multi_yaml_unsupported)
		end,
		arweave_config_format_dotted:to_leaf_map(proplist_to_map(Proplist))
	catch
		throw:Reason -> {error, Reason};
		_:Reason -> {error, Reason}
	end.

%% @doc Recursively convert a `yamerl` proplist into a nested map.
-spec proplist_to_map(Proplist) -> Return when
	Proplist :: term(),
	Return :: map().
proplist_to_map(Proplist) ->
	proplist_to_map(Proplist, #{}).

proplist_to_map([], Buffer) ->
	Buffer;
proplist_to_map([{K,V = [{_,_}|_]}|Rest], Buffer) ->
	%% A list of tuples is a nested YAML mapping; recurse.
	Recurse = proplist_to_map(V),
	Key = arweave_config_parser:format_segment(K),
	proplist_to_map(Rest, Buffer#{ Key => Recurse });
proplist_to_map([{K, V}|Rest], Buffer) when is_list(V) ->
	Key = arweave_config_parser:format_segment(K),
	case io_lib:printable_unicode_list(V) of
		true ->
			proplist_to_map(Rest, Buffer#{ Key => list_to_binary(V) });
		false ->
			Value = [decode_yaml_item(E) || E <- V],
			proplist_to_map(Rest, Buffer#{ Key => Value })
	end;
proplist_to_map([{K,V}|Rest], Buffer) ->
	Key = arweave_config_parser:format_segment(K),
	Value = decode_value(V),
	proplist_to_map(Rest, Buffer#{ Key => Value }).

decode_value(true) ->
	true;
decode_value(false) ->
	false;
decode_value(null) ->
	null;
decode_value(Value) when is_atom(Value) ->
	atom_to_binary(Value);
decode_value(Value) when is_list(Value) ->
	list_to_binary(Value);
decode_value(Value) ->
	Value.

%% A list element coming back from `yamerl` is either:
%%   - a proplist (a YAML mapping inside a sequence) - convert to a map,
%%   - or a scalar — decode via `decode_value/1`.
decode_yaml_item([{_, _} | _] = Proplist) ->
	proplist_to_map(Proplist);
decode_yaml_item(Other) ->
	decode_value(Other).

%%====================================================================
%% YAML Encoder
%%====================================================================

%% @doc Encode a nested Erlang map as a YAML binary.
%% @see encode/2
-spec encode(Map) -> Return when
	Map :: map(),
	Return :: {ok, binary()} | {error, term()}.
encode(Map) ->
	encode(Map, #{}).

%% @doc Encode a nested Erlang map as a YAML binary.
-spec encode(Map, Opts) -> Return when
	Map :: map(),
	Opts :: map(),
	Return :: {ok, binary()} | {error, term()}.
encode(Map, _Opts) when is_map(Map) ->
	try
		IoList = encode_map(Map, 0),
		{ok, iolist_to_binary(IoList)}
	catch
		_:Reason -> {error, Reason}
	end;
encode(_, _) ->
	{error, badarg}.

%% @doc Keys are sorted alphabetically for deterministic output.
encode_map(Map, Indent) ->
	Keys = lists:sort(maps:keys(Map)),
	lists:map(
		fun(Key) ->
			Value = maps:get(Key, Map),
			KeyBin = arweave_config_parser:format_segment(Key),
			Prefix = indent(Indent),
			encode_pair(Prefix, KeyBin, Value, Indent)
		end,
		Keys
	).

encode_pair(Prefix, KeyBin, Value, Indent) when is_map(Value) ->
	[Prefix, KeyBin, <<":\n">>, encode_map(Value, Indent + 1)];
encode_pair(Prefix, KeyBin, Value, Indent) when is_list(Value) ->
	[Prefix, KeyBin, <<":\n">>, encode_list(Value, Indent + 1)];
encode_pair(Prefix, KeyBin, Value, _Indent) ->
	[Prefix, KeyBin, <<": ">>, encode_scalar(Value), <<"\n">>].

encode_list(List, Indent) ->
	Prefix = indent(Indent),
	lists:map(
		fun(Item) when is_map(Item) ->
			%% Inline the first map entry onto the dash line so the
			%% sequence item and the mapping share a row.
			[{FirstK, FirstV}|Rest] = lists:sort(maps:to_list(Item)),
			FirstLine = [Prefix, <<"- ">>,
				arweave_config_parser:format_segment(FirstK), <<": ">>,
				encode_scalar(FirstV), <<"\n">>],
			RestLines = lists:map(
				fun({K, V}) ->
					[Prefix, <<"  ">>,
						arweave_config_parser:format_segment(K), <<": ">>,
						encode_scalar(V), <<"\n">>]
				end,
				lists:sort(Rest)
			),
			[FirstLine|RestLines];
		(Item) ->
			[Prefix, <<"- ">>, encode_scalar(Item), <<"\n">>]
		end,
		List
	).

encode_scalar(true) -> <<"true">>;
encode_scalar(false) -> <<"false">>;
encode_scalar(null) -> <<"null">>;
encode_scalar(undefined) -> <<"null">>;
encode_scalar(infinity) -> <<".inf">>;
encode_scalar(Value) when is_integer(Value) ->
	integer_to_binary(Value);
encode_scalar(Value) when is_float(Value) ->
	float_to_binary(Value, [{decimals, 10}, compact]);
encode_scalar(Value) when is_atom(Value) ->
	yaml_quote(atom_to_binary(Value));
encode_scalar(Value) when is_binary(Value) ->
	yaml_quote(Value);
encode_scalar(Value) when is_list(Value) ->
	yaml_quote(list_to_binary(Value));
encode_scalar(Value) ->
	yaml_quote(iolist_to_binary(io_lib:format("~p", [Value]))).

%% @doc Quote when the value contains YAML-special characters or could
%% be misread as a non-string scalar (number, boolean, null).
yaml_quote(<<>>) -> <<"\"\"">>;
yaml_quote(Bin) ->
	case needs_quoting(Bin) of
		true -> <<"\"", (yaml_escape(Bin))/binary, "\"">>;
		false -> Bin
	end.

needs_quoting(Bin) ->
	is_yaml_reserved(Bin) orelse
	is_numeric_string(Bin) orelse
	has_special_chars(Bin).

is_yaml_reserved(<<"true">>) -> true;
is_yaml_reserved(<<"false">>) -> true;
is_yaml_reserved(<<"null">>) -> true;
is_yaml_reserved(<<"yes">>) -> true;
is_yaml_reserved(<<"no">>) -> true;
is_yaml_reserved(<<"on">>) -> true;
is_yaml_reserved(<<"off">>) -> true;
is_yaml_reserved(<<"True">>) -> true;
is_yaml_reserved(<<"False">>) -> true;
is_yaml_reserved(<<"Yes">>) -> true;
is_yaml_reserved(<<"No">>) -> true;
is_yaml_reserved(<<"NULL">>) -> true;
is_yaml_reserved(<<"~">>) -> true;
is_yaml_reserved(<<".inf">>) -> true;
is_yaml_reserved(<<"-.inf">>) -> true;
is_yaml_reserved(<<".nan">>) -> true;
is_yaml_reserved(_) -> false.

is_numeric_string(Bin) ->
	try
		_ = binary_to_integer(Bin),
		true
	catch _:_ ->
		try
			_ = binary_to_float(Bin),
			true
		catch _:_ -> false
		end
	end.

has_special_chars(Bin) ->
	binary:match(Bin, [<<":">>, <<"#">>, <<"{">>, <<"}">>,
		<<"[">>, <<"]">>, <<",">>, <<"&">>, <<"*">>,
		<<"?">>, <<"|">>, <<"-">>, <<"<">>, <<">">>,
		<<"=">>, <<"!">>, <<"%">>, <<"@">>, <<"`">>,
		<<"\"">>, <<"'">>, <<"\n">>, <<"\r">>, <<"\t">>]) =/= nomatch
	orelse
	%% Leading or trailing whitespace also forces quoting.
	case Bin of
		<<" ", _/binary>> -> true;
		_ ->
			Size = byte_size(Bin) - 1,
			case Bin of
				<<_:Size/binary, " ">> -> true;
				_ -> false
			end
	end.

yaml_escape(Bin) ->
	yaml_escape(Bin, <<>>).

yaml_escape(<<>>, Acc) -> Acc;
yaml_escape(<<"\"", Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, "\\\"">>);
yaml_escape(<<"\\", Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, "\\\\">>);
yaml_escape(<<"\n", Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, "\\n">>);
yaml_escape(<<"\r", Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, "\\r">>);
yaml_escape(<<"\t", Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, "\\t">>);
yaml_escape(<<C, Rest/binary>>, Acc) ->
	yaml_escape(Rest, <<Acc/binary, C>>).

%% @doc Two spaces per indent level.
indent(0) -> <<>>;
indent(N) -> binary:copy(<<"  ">>, N).
