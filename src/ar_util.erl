-module(ar_util).
-export([pick_random/1, pick_random/2]).
-export([hexify/1, dehexify/1]).
-export([encode/1, decode/1]).
-export([encode_base64_safe/1, decode_base64_safe/1]).
-export([parse_peer/1, parse_port/1, format_peer/1, unique/1, count/2]).
-export([replace/3]).
-export([block_from_hash_list/2, hash_from_hash_list/2, get_recall_hash/2]).
-export([height_from_hashes/1, wallets_from_hashes/1, blocks_from_hashes/1]).
-export([get_hash/1, get_head_block/1]).
-export([genesis_wallets/0]).
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
	encode_base64_safe(base64:encode_to_string(Bin)).

%% @doc Decode a URL safe base64 to binary.
decode(Bin) when is_binary(Bin) ->
	decode(bitstring_to_list(Bin));
decode(Str) ->
	base64:decode(decode_base64_safe(Str)).

%% @doc Takes base64 and encodes it into base64 suitable for URLs.
encode_base64_safe([]) -> [];
encode_base64_safe([$=|T]) ->
	encode_base64_safe(T);
encode_base64_safe([$/|T]) ->
	[ $_ | encode_base64_safe(T) ];
encode_base64_safe([$+|T]) ->
	[ $- | encode_base64_safe(T) ];
encode_base64_safe([H|T]) ->
	[ H | encode_base64_safe(T) ].

%% @doc Decodes URL safe base64 and turns it back into base64.
decode_base64_safe(Str) ->
	% TODO: Make this efficient.
	UnsafeStr = do_decode_base64_safe(Str),
	lists:flatten(
		string:pad(
			UnsafeStr,
			length(UnsafeStr)
				% TODO: Make this 100% less terrible.
				+ case 4 - (length(UnsafeStr) rem 4) of
					4 -> 0;
					X -> X
				end,
			trailing,
			$=
		)
	).

do_decode_base64_safe([]) -> [];
do_decode_base64_safe([$_|T]) ->
	[ $/ | do_decode_base64_safe(T) ];
do_decode_base64_safe([$-|T]) ->
	[ $+ | do_decode_base64_safe(T) ];
do_decode_base64_safe([H|T]) ->
	[ H | do_decode_base64_safe(T) ].

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
	lists:map(fun ar_storage:read_block/1, BHL).

%% @doc Fetch a block hash by number from a block hash list (and disk).
hash_from_hash_list(Num, BHL) ->
	lists:nth(Num - 1, lists:reverse(BHL)).

block_from_hash_list(Num, BHL) ->
	ar_storage:read_block(hash_from_hash_list(Num, BHL)).

%% @doc Fetch the head block using BHL.
get_head_block(not_joined) -> unavailable;
get_head_block([IndepHash|_]) ->
	ar_storage:read_block(IndepHash).

%% @doc find the hash of a recall block.
get_recall_hash(B, HashList) ->
	lists:nth(1 + ar_weave:calculate_recall_block(B), lists:reverse(HashList)).

%% @doc Replace a term in a list with another term.
replace(_, _, []) -> [];
replace(X, Y, [X|T]) -> [Y|replace(X, Y, T)];
replace(X, Y, [H|T]) -> [H|replace(X, Y, T)].

%% @doc Convert a binary to a list of hex characters.
hexify(Bin) ->
	lists:flatten(
		[
			io_lib:format("~2.16.0B", [X])
		||
    		X <- binary_to_list(Bin)
		]
	).

%% @doc Turn a list of hex characters into a binary.
dehexify(Bin) when is_binary(Bin) ->
	dehexify(binary_to_list(Bin));
dehexify(S) ->
	dehexify(S, []).
dehexify([], Acc) ->
	list_to_binary(lists:reverse(Acc));
dehexify([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	dehexify(T, [V | Acc]).

%% @doc Parse a string representing a remote host into our internal format.
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
unique(Xs) -> unique([], Xs).
unique(Res, []) -> lists:reverse(Res);
unique(Res, [X|Xs]) ->
	case lists:member(X, Res) of
		false -> unique([X|Res], Xs);
		true -> unique(Res, Xs)
	end.

%% @doc Test that unique functions correctly.
basic_unique_test() ->
	[a, b, c] = unique([a, a, b, b, b, c, c]).

%% @doc Ensure that hosts are formatted as lists correctly.
basic_peer_format_test() ->
	"127.0.0.1:9001" = format_peer({127,0,0,1,9001}).

%% @doc Test that values can be hexed and dehexed.
round_trip_hexify_test() ->
	Bytes = dehexify(hexify(Bytes = crypto:strong_rand_bytes(32))).

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

%% @doc Generate a list of GENESIS wallets, from the CSV file.
genesis_wallets() ->
	{ok, Bin} = file:read_file("data/genesis_wallets.csv"),
	lists:map(
		fun(Line) ->
			[PubKey, RawQty] = string:tokens(Line, ","),
			{
				ar_util:decode(PubKey),
				erlang:trunc(math:ceil(list_to_integer(RawQty)))
			}
		end,
		string:tokens(binary_to_list(Bin), [10])
	).
