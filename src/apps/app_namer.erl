-module(app_namer).
-export([start/1, start/2, stop/1]).

%%% Generate random wallets until you find one that starts with the given 
%%% characters.

%% @doc Start a new name searcher, looking for a wallet that starts with the
%% given characters. Optionally takes a number of cores to search with.
start(Str) -> start(Str, 1).
start(Str, Cores) ->
    [ spawn(fun() -> server(Str) end) || _ <- lists:seq(1, Cores) ].

%% @doc Stops all of the processes in a name search.
stop(PIDs) ->
    lists:foreach(fun(PID) -> PID ! stop end, PIDs).

%% @doc Main server body. Check for stop, then make a guess.
server(Str) ->
    receive
        stop -> ok
    after 0 ->
        maybe_save(Str, guess()),
        server(Str)
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
maybe_save(Str, Addr) ->
    case lists:prefix(Str, Addr) of
        false ->
            file:delete("wallets/arweave_keyfile_" ++ Addr ++ ".json");
        true ->
            io:format("Found wallet ~s!~n", [Addr])
    end.