%%%
%%% @doc Useful helpers for the work in Arweave.
%%%

-module(ar_util).

-export([pick_random/1, pick_random/2]).
-export([encode/1, decode/1]).
-export([parse_peer/1, parse_port/1, format_peer/1, unique/1, count/2]).
-export([replace/3]).
-export([block_from_hash_list/2, hash_from_hash_list/2]).
-export([get_recall_hash/2, get_recall_hash/3]).
-export([height_from_hashes/1, wallets_from_hashes/1, blocks_from_hashes/1]).
-export([get_hash/1, get_head_block/1]).
-export([genesis_wallets/0]).
-export([pmap/2]).
-export([time_difference/2]).
-export([rev_bin/1]).
-export([do_until/3]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% General misc. utility functions. Most of these should probably
%%% have been in the Erlang standard library...

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
	base64url:encode(Bin).

%% @doc Decode a URL safe base64 to binary.
decode([]) -> [];
decode(Bin) when is_list(Bin) ->
	decode(list_to_binary(Bin));
decode(Bin) when is_binary(Bin) ->
	base64url:decode(Bin).

%% @doc Reverse a binary
rev_bin(Bin) ->
    rev_bin(Bin, <<>>).
rev_bin(<<>>, Acc) -> Acc;
rev_bin(<<H:1/binary, Rest/binary>>, Acc) ->
    rev_bin(Rest, <<H/binary, Acc/binary>>).

%% @doc Get a block's hash.
get_hash(B) when is_record(B, block) ->
	B#block.indep_hash.

%% @doc Get block height from a hash list.
height_from_hashes(not_joined) -> -1;
height_from_hashes(BHL) ->
	(get_head_block(BHL))#block.height.

%% @doc Get a wallet list from a hash list.
wallets_from_hashes(not_joined) -> [];
wallets_from_hashes(HashList) ->
	(get_head_block(HashList))#block.wallet_list.

%% @doc Get block list from hash list.
blocks_from_hashes([]) -> undefined;
blocks_from_hashes(BHL) ->
	lists:map(fun(BH) -> ar_storage:read_block(BH, BHL) end, BHL).

%% @doc Fetch a block hash by number from a block hash list (and disk).
hash_from_hash_list(Num, BHL) ->
	lists:nth(Num - 1, lists:reverse(BHL)).

%% @doc Read a block at the given height from the hash list
block_from_hash_list(Num, BHL) ->
	ar_storage:read_block(hash_from_hash_list(Num, BHL), BHL).

%% @doc Fetch the head block using BHL.
get_head_block(not_joined) -> unavailable;
get_head_block(BHL = [IndepHash|_]) ->
	ar_storage:read_block(IndepHash, BHL).

%% @doc find the hash of a recall block.
get_recall_hash(B, HashList) ->
	lists:nth(
        1 + ar_weave:calculate_recall_block(B, HashList),
        lists:reverse(HashList)
    ).
get_recall_hash(_Height, Hash, []) -> Hash;
get_recall_hash(0, Hash, _HastList) -> Hash;
get_recall_hash(Height, Hash, HashList) ->
	lists:nth(
        1 + ar_weave:calculate_recall_block(Hash, Height, HashList),
        lists:reverse(HashList)
    ).

%% @doc Replace a term in a list with another term.
replace(_, _, []) -> [];
replace(X, Y, [X|T]) -> [Y|replace(X, Y, T)];
replace(X, Y, [H|T]) -> [H|replace(X, Y, T)].

%% @doc Parse a string representing a remote host into our internal format.
parse_peer("") -> throw(empty_peer_string);
parse_peer(BitStr) when is_bitstring(BitStr) ->
	parse_peer(bitstring_to_list(BitStr));
parse_peer(Str) when is_list(Str) ->
	case io_lib:fread("~d.~d.~d.~d", Str) of
		{ok, [A, B, C, D], PortStr} ->
			{A, B, C, D, parse_port(PortStr)};
		{error, _} ->
			{127, 0, 0, 1, parse_port(Str)}
	end;
parse_peer({IP, Port}) ->
	{A, B, C, D} = parse_peer(IP),
	{A, B, C, D, parse_port(Port)}.

%% @doc Parses a port string into an integer.
parse_port(Int) when is_integer(Int) -> Int;
parse_port("") -> ?DEFAULT_HTTP_IFACE_PORT;
parse_port(PortStr) ->
	{ok, [Port], ""} = io_lib:fread(":~d", PortStr),
	Port.

%% @doc Take a remote host ID in various formats, return a HTTP-friendly string.
format_peer(undefined) ->
	ar:report(caught_undefined_host);
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
%% NOTE: Does not maintain list stability.
pmap(Fun, List) ->
	Master = self(),
	lists:map(
		fun(_) ->
			receive
				{pmap_work, X} -> X
			end
		end,
		lists:map(fun(Elem) -> Master ! {pmap_work, Fun(Elem)} end, List)
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

%% @doc Generates the difference in microseconds between two erlang time tuples
time_difference({M1, S1, U1}, {M2, S2, U2}) ->
	((M1-M2) * 1000000000000) + ((S1-S2) * 1000000) + (U1 - U2);
time_difference(_,_) ->
	bad_time_format.

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
	Res = pmap(fun(X) -> receive after 500 -> X * 2 end end, [1, 5, 10]),
	true = lists:member(2, Res),
	true = lists:member(10, Res),
	true = lists:member(20, Res).

recall_block_test() ->
	ar_storage:clear(),
	Node = ar_node:start(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node ! {replace_block_list, B0},
	receive after 300 -> ok end,
	B1 = ar_node:get_current_block(Node),
	ar_node:mine(Node),
	receive after 300 -> ok end,
	B3 = ar_node:get_current_block(Node),
	B3#block.wallet_list.

%%%
%%% EOF
%%%
