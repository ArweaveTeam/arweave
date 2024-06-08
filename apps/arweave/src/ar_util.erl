-module(ar_util).

-export([bool_to_int/1, int_to_bool/1, ceil_int/2, floor_int/2, between/3,
		integer_to_binary/1, binary_to_integer/1, pick_random/1, pick_random/2,
		encode/1, decode/1, safe_encode/1, safe_decode/1, timestamp_to_seconds/1,
		parse_peer/1, peer_to_str/1, parse_port/1, safe_parse_peer/1, format_peer/1,
		unique/1, count/2,
		genesis_wallets/0, pmap/2, pfilter/2,
		do_until/3, block_index_entry_from_block/1,
		bytes_to_mb_string/1, cast_after/3, encode_list_indices/1, parse_list_indices/1,
		take_every_nth/2, safe_divide/2, terminal_clear/0, print_stacktrace/0, shuffle_list/1,
		assert_file_exists_and_readable/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

bool_to_int(true) -> 1;
bool_to_int(_) -> 0.

int_to_bool(1) -> true;
int_to_bool(0) -> false.

%% @doc Implementations of integer_to_binary and binary_to_integer that can handle infinity.
integer_to_binary(infinity) ->
	<<"infinity">>;
integer_to_binary(N) ->
	erlang:integer_to_binary(N).

binary_to_integer(<<"infinity">>) ->
	infinity;
binary_to_integer(N) ->
	erlang:binary_to_integer(N).

%% @doc: rounds IntValue up to the nearest multiple of Nearest.
%% Rounds up even if IntValue is already a multiple of Nearest.
ceil_int(IntValue, Nearest) ->
	IntValue - (IntValue rem Nearest) + Nearest.

%% @doc: rounds IntValue down to the nearest multiple of Nearest.
%% Doesn't change IntValue if it's already a multiple of Nearest.
floor_int(IntValue, Nearest) ->
	IntValue - (IntValue rem Nearest).

%% @doc: clamp N to be between Min and Max.
between(N, Min, _) when N < Min -> Min;
between(N, _, Max) when N > Max -> Max;
between(N, _, _) -> N.

%% @doc Pick a list of random elements from a given list.
pick_random(_, 0) -> [];
pick_random([], _) -> [];
pick_random(List, N) ->
	Elem = pick_random(List),
	[Elem|pick_random(List -- [Elem], N - 1)].

%% @doc Select a random element from a list.
pick_random(Xs) ->
	lists:nth(rand:uniform(length(Xs)), Xs).

%% @doc Encode a binary to URL safe base64 binary string.
encode(Bin) ->
	b64fast:encode(Bin).

%% @doc Try to decode a URL safe base64 into a binary or throw an error when
%% invalid.
decode(Input) ->
	b64fast:decode(Input).

safe_encode(Bin) when is_binary(Bin) ->
	encode(Bin);
safe_encode(Bin) ->
	Bin.

%% @doc Safely decode a URL safe base64 into a binary returning an ok or error
%% tuple.
safe_decode(E) ->
	try
		D = decode(E),
		{ok, D}
	catch
		_:_ ->
			{error, invalid}
	end.

%% @doc Convert an erlang:timestamp() to seconds since the Unix Epoch.
timestamp_to_seconds({MegaSecs, Secs, _MicroSecs}) ->
	MegaSecs * 1000000 + Secs.

%% @doc Parse a string representing a remote host into our internal format.
parse_peer("") -> throw(empty_peer_string);
parse_peer(BitStr) when is_bitstring(BitStr) ->
	parse_peer(bitstring_to_list(BitStr));
parse_peer(Str) when is_list(Str) ->
    [Addr, PortStr] = parse_port_split(Str),
    case inet:getaddr(Addr, inet) of
		{ok, {A, B, C, D}} ->
			{A, B, C, D, parse_port(PortStr)};
		{error, Reason} ->
			throw({invalid_peer_string, Str, Reason})
	end;
parse_peer({IP, Port}) ->
	{A, B, C, D} = parse_peer(IP),
	{A, B, C, D, parse_port(Port)}.

peer_to_str(Bin) when is_binary(Bin) ->
	binary_to_list(Bin);
peer_to_str(Str) when is_list(Str) ->
	Str;
peer_to_str({A, B, C, D, Port}) ->
	integer_to_list(A) ++ "_" ++ integer_to_list(B) ++ "_" ++ integer_to_list(C) ++ "_"
			++ integer_to_list(D) ++ "_" ++ integer_to_list(Port).

%% @doc Parses a port string into an integer.
parse_port(Int) when is_integer(Int) -> Int;
parse_port("") -> ?DEFAULT_HTTP_IFACE_PORT;
parse_port(PortStr) ->
	{ok, [Port], ""} = io_lib:fread("~d", PortStr),
	Port.

parse_port_split(Str) ->
    case string:tokens(Str, ":") of
        [Addr] -> [Addr, ?DEFAULT_HTTP_IFACE_PORT];
        [Addr, Port] -> [Addr, Port];
        _ -> throw({invalid_peer_string, Str})
    end.

safe_parse_peer(Peer) ->
	try
		{ok, parse_peer(Peer)}
	catch
		_:_ -> {error, invalid}
	end.

%% @doc Take a remote host ID in various formats, return a HTTP-friendly string.
format_peer({A, B, C, D}) ->
	format_peer({A, B, C, D, ?DEFAULT_HTTP_IFACE_PORT});
format_peer({A, B, C, D, Port}) ->
	lists:flatten(io_lib:format("~w.~w.~w.~w:~w", [A, B, C, D, Port]));
format_peer(Host) when is_list(Host) ->
	format_peer({Host, ?DEFAULT_HTTP_IFACE_PORT});
format_peer({Host, Port}) ->
	lists:flatten(io_lib:format("~s:~w", [Host, Port]));
format_peer(Peer) ->
	Peer.

%% @doc Count occurences of element within list.
count(A, List) ->
	length([ B || B <- List, A == B ]).

%% @doc Takes a list and return the unique values in it.
unique(Xs) when not is_list(Xs) ->
[Xs];
unique(Xs) -> unique([], Xs).
unique(Res, []) -> lists:reverse(Res);
unique(Res, [X|Xs]) ->
	case lists:member(X, Res) of
		false -> unique([X|Res], Xs);
		true -> unique(Res, Xs)
	end.

%% @doc Run a map in parallel.
%% NOTE: Make this efficient for large lists.
pmap(Mapper, List) ->
	Master = self(),
	ListWithRefs = [{Elem, make_ref()} || Elem <- List],
	lists:foreach(fun({Elem, Ref}) ->
		spawn_link(fun() ->
			process_flag(trap_exit, true),
			Master ! {pmap_work, Ref, Mapper(Elem)}
		end)
	end, ListWithRefs),
	lists:map(
		fun({_, Ref}) ->
			receive
				{pmap_work, Ref, Mapped} -> Mapped
			end
		end,
		ListWithRefs
	).

%% @doc Filter the list in parallel.
pfilter(Fun, List) ->
	Master = self(),
	ListWithRefs = [{Elem, make_ref()} || Elem <- List],
	lists:foreach(fun({Elem, Ref}) ->
		spawn_link(fun() ->
			process_flag(trap_exit, true),
			Master ! {pmap_work, Ref, Fun(Elem)}
		end)
	end, ListWithRefs),
	lists:filtermap(
		fun({Elem, Ref}) ->
			receive
				{pmap_work, Ref, false} -> false;
				{pmap_work, Ref, true} -> {true, Elem};
				{pmap_work, Ref, {true, Result}} -> {true, Result}
			end
		end,
		ListWithRefs
	).

%% @doc Generate a list of GENESIS wallets, from the CSV file.
genesis_wallets() ->
	{ok, Bin} = file:read_file("data/genesis_wallets.csv"),
	lists:map(
		fun(Line) ->
			[Addr, RawQty] = string:tokens(Line, ","),
			{
				ar_util:decode(Addr),
				erlang:trunc(math:ceil(list_to_integer(RawQty))) * ?WINSTON_PER_AR,
				<<>>
			}
		end,
		string:tokens(binary_to_list(Bin), [10])
	).

%% @doc Perform a function until it returns {ok, Value} | ok | true | {error, Error}.
%% That term will be returned, others will be ignored. Interval and timeout have to
%% be passed in milliseconds.
do_until(_DoFun, _Interval, Timeout) when Timeout =< 0 ->
	{error, timeout};
do_until(DoFun, Interval, Timeout) ->
	Start = erlang:system_time(millisecond),
	case DoFun() of
		{ok, Value} ->
			{ok, Value};
		ok ->
			ok;
		true ->
			true;
		{error, Error} ->
			{error, Error};
		_ ->
			timer:sleep(Interval),
			Now = erlang:system_time(millisecond),
			do_until(DoFun, Interval, Timeout - (Now - Start))
	end.

block_index_entry_from_block(B) ->
	{B#block.indep_hash, B#block.weave_size, B#block.tx_root}.

%% @doc Convert the given number of bytes into the "%s MiB" string.
bytes_to_mb_string(Bytes) ->
	integer_to_list(Bytes div 1024 div 1024) ++ " MiB".

%% @doc Encode the given list of sorted numbers into a binary where the nth bit
%% is 1 the corresponding number is present in the given list; 0 otherwise.
encode_list_indices(Indices) ->
	encode_list_indices(Indices, 0).

encode_list_indices([Index | Indices], N) ->
	<< 0:(Index - N), 1:1, (encode_list_indices(Indices, Index + 1))/bitstring >>;
encode_list_indices([], N) when N rem 8 == 0 ->
	<<>>;
encode_list_indices([], N) ->
	<< 0:(8 - N rem 8) >>.

%% @doc Return a list of position numbers corresponding to 1 bits of the given binary.
parse_list_indices(Input) ->
	parse_list_indices(Input, 0).

parse_list_indices(<< 0:1, Rest/bitstring >>, N) ->
	parse_list_indices(Rest, N + 1);
parse_list_indices(<< 1:1, Rest/bitstring >>, N) ->
	case parse_list_indices(Rest, N + 1) of
		error ->
			error;
		Indices ->
			[N | Indices]
	end;
parse_list_indices(<<>>, _N) ->
	[];
parse_list_indices(_BadInput, _N) ->
	error.

shuffle_list(List) ->
	lists:sort(fun(_,_) -> rand:uniform() < 0.5 end, List).

%%%
%%% Tests.
%%%

%% @doc Test that unique functions correctly.
basic_unique_test() ->
	[a, b, c] = unique([a, a, b, b, b, c, c]).

%% @doc Ensure that hosts are formatted as lists correctly.
basic_peer_format_test() ->
	"127.0.0.1:9001" = format_peer({127,0,0,1,9001}).

%% @doc Ensure that pick_random's are actually in the starting list.
pick_random_test() ->
	List = [a, b, c, d, e],
	true = lists:member(pick_random(List), List).

%% @doc Test that binaries of different lengths can be encoded and decoded
%% correctly.
round_trip_encode_test() ->
	lists:map(
		fun(Bytes) ->
			Bin = crypto:strong_rand_bytes(Bytes),
			Bin = decode(encode(Bin))
		end,
		lists:seq(1, 64)
	).

%% Test the paralell mapping functionality.
pmap_test() ->
	Mapper = fun(X) ->
		timer:sleep(100 * X),
		X * 2
	end,
	?assertEqual([6, 2, 4], pmap(Mapper, [3, 1, 2])).

cast_after(Delay, Module, Message) ->
	%% Not using timer:apply_after here because send_after is more efficient:
	%% http://erlang.org/doc/efficiency_guide/commoncaveats.html#timer-module.
	erlang:send_after(Delay, Module, {'$gen_cast', Message}).

take_every_nth(N, L) ->
	take_every_nth(N, L, 0).

take_every_nth(_N, [], _I) ->
	[];
take_every_nth(N, [El | L], I) when I rem N == 0 ->
	[El | take_every_nth(N, L, I + 1)];
take_every_nth(N, [_El | L], I) ->
	take_every_nth(N, L, I + 1).

safe_divide(A, B) ->
	case catch A / B of
		{'EXIT', _} ->
			A div B;
		Result ->
			Result
	end.

encode_list_indices_test() ->
	lists:foldl(
		fun(Input, N) ->
			?assertEqual(Input, lists:sort(Input)),
			Encoded = encode_list_indices(Input),
			?assert(byte_size(Encoded) =< 125),
			Indices = parse_list_indices(Encoded),
			?assertEqual(Input, Indices, io_lib:format("Case ~B", [N])),
			N + 1
		end,
		0,
		[[], [0], [1], [999], [0, 1], lists:seq(0, 999), lists:seq(0, 999, 2),
			lists:seq(1, 999, 3)]
	).

%% @doc os aware way of clearing a terminal
terminal_clear() ->
	io:format(
		case os:type() == "darwin" of
			true -> "\e[H\e[J";
			false ->  os:cmd(clear)
		end
	).

print_stacktrace() ->
    try
        throw(dummy) %% In OTP21+ try/catch is the recommended way to get the stacktrace
    catch
        _: _Exception:Stacktrace ->
            %% Remove the first element (print_stacktrace call)
            TrimmedStacktrace = lists:nthtail(1, Stacktrace),
			StacktraceString = lists:foldl(
				fun(StackTraceEntry, Acc) ->
                	Acc ++ io_lib:format("  ~p~n", [StackTraceEntry])
                end, "Stack trace:~n", TrimmedStacktrace),
			?LOG_INFO(StacktraceString)
    end.

% Function to assert that a file exists and is readable
assert_file_exists_and_readable(FilePath) ->
	case file:read_file(FilePath) of
		{ok, _} ->
			ok;
		{error, _} ->
			io:format("~nThe filepath ~p doesn't exist or isn't readable.~n~n", [FilePath]),
			erlang:halt(1)
	end.
