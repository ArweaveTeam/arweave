-module(app_namer).
-export([start/1, start/2, start/3, stop/1]).

%%% Generate random wallets until you find one that starts with the given 
%%% characters.

%% @doc Start a new name searcher, looking for a wallet that starts with the
%% given characters. Optionally takes a number of cores to search with.
start(Str) -> start(Str, 1).
start(Str, Cores) -> start(Str, Cores, normal).
start(Str, Cores, Type) ->
    [ spawn(fun() -> server(Type, Str) end) || _ <- lists:seq(1, Cores) ].

%% @doc Stops all of the processes in a name search.
stop(PIDs) ->
    lists:foreach(fun(PID) -> PID ! stop end, PIDs).

%% @doc Main server body. Check for stop, then make a guess.
server(Type, Str) ->
    receive
        stop -> ok
    after 0 ->
        maybe_save(Type, Str, guess()),
        server(Type, Str)
    end.

%% @doc Generate a new wallet and its address.
guess() ->
    binary_to_list(
        ar_util:encode(
            ar_wallet:to_address(
                ar_wallet:new_keyfile()
            )
        )
    ).

%% @doc If the wallet is a hit, save it and notify the user.
maybe_save(Type, Str, Addr) ->
    case matches(Type, Str, Addr) of
        false ->
            file:delete("wallets/arweave_keyfile_" ++ Addr ++ ".json");
        true ->
            io:format("Found wallet ~s!~n", [Addr])
    end.

%% @doc Check whether an address matches a query.
matches(strict, Str, Addr) ->
    lists:prefix(Str, Addr);
matches(normal, Str, Addr) ->
    matches(strict, string:to_lower(Str), string:to_lower(Addr));
matches(txt, Str, Addr) ->
    matches(normal, base55(Str), base55(Addr));
matches(l33t, Str, Addr) ->
    matches(normal, base55(Str), base55(Addr)).

%% @doc Minimise the base 64 state space 55.
base55(Str) when is_list(Str) ->lists:map(fun base55/1, Str);
base55($1) -> $i;
base55($3) -> $e;
base55($4) -> $a;
base55($5) -> $s;
base55($7) -> $l;
base55($9) -> $g;
base55($0) -> $o;
base55($-) -> $ ;
base55($_) -> $ ;
base55(X) -> X.