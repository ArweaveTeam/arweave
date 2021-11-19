-module(ar_util).

-export([pick_random/1, pick_random/2,
		encode/1, decode/1, safe_decode/1,
		parse_peer/1, parse_port/1, safe_parse_peer/1, format_peer/1, unique/1, count/2,
		genesis_wallets/0, pmap/2, pfilter/2,
		do_until/3, block_index_entry_from_block/1, get_block_index_intersection/2,
		reset_peer/1, get_performance/1, update_timer/1, bytes_to_mb_string/1, cast_after/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Pick a list of random elements from a given list.
pick_random(_, 0) -> [];
pick_random([], _) -> [];
pick_random(List, N) ->
	Elem = pick_random(List),
	[Elem|pick_random(List -- [Elem], N - 1)].

%% @doc Select a random element from a list.
pick_random(Xs) ->
	lists:nth(rand:uniform(length(Xs)), Xs).

%% @doc Encode a binary to URL safe base64.
encode(Bin) ->
	b64fast:encode(Bin).

%% @doc Try to decode a URL safe base64 into a binary or throw an error when
%% invalid.
decode(Input) ->
	b64fast:decode(Input).

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
	lists:flatten(io_lib:format("~s:~w", [Host, Port])).

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
			[PubKey, RawQty] = string:tokens(Line, ","),
			{
				ar_wallet:to_address(ar_util:decode(PubKey)),
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

%% @doc Find the latest block from the given block index (the second argument) that
%% is inside the other given block index (the first argument). Return none if no entry
%% from the second list belongs to the first list.
get_block_index_intersection(BI, CurrentBI) ->
	Height = length(BI) - 1,
	get_block_index_intersection(BI, CurrentBI, Height).

get_block_index_intersection(BI, [{BH, _WeaveSize, _TXRoot} = Entry | CurrentBI], Height) ->
	case block_index_contains_block(BI, BH, Height) of
		{true, TipHeight} ->
			{Entry, TipHeight};
		false ->
			get_block_index_intersection(BI, CurrentBI, Height)
	end;
get_block_index_intersection(_BI, [], _Height) ->
	none.

block_index_contains_block([{BH, _, _} | _], BH, Height) ->
	{true, Height};
block_index_contains_block([_ | BI], BH, Height) ->
	block_index_contains_block(BI, BH, Height - 1);
block_index_contains_block([], _BH, _Height) ->
	false.

%% @doc Reset the performance data for a given peer.
reset_peer(Peer = {_, _, _, _, _}) ->
	ar_meta_db:put({peer, Peer}, #performance{}).

%% @doc Return the performance object for a node.
get_performance(Peer = {_, _, _, _, _}) ->
	case ar_meta_db:get({peer, Peer}) of
		not_found -> #performance{};
		P -> P
	end.

%% @doc Update the "last on list" timestamp of a given peer
update_timer(Peer = {_, _, _, _, _}) ->
	case ar_meta_db:get({peer, Peer}) of
		not_found ->
			ok;
		P ->
			ar_meta_db:put({peer, Peer},
				P#performance {
					transfers = P#performance.transfers,
					time = P#performance.time ,
					bytes = P#performance.bytes,
					timestamp = os:system_time(seconds)
				}
			)
	end.

%% @doc Convert the given number of bytes into the "%s MiB" string.
bytes_to_mb_string(Bytes) ->
	integer_to_list(Bytes div 1024 div 1024) ++ " MiB".

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
