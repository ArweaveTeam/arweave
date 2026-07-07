-module(arweave_throttling_path).

-export([path_to_path_key/1, path_to_group_id/2]).

%% @doc get a simplified version of the path (no variables).
path_to_path_key(Path) ->
    split_path_to_path_key(split_path(Path)).

%% @doc find throttling group ID for a certain request (Peer, Path) pair.
%%
path_to_group_id(Peer, Path) ->
    case path_to_path_key(Path) of
        {error, skip} ->
            {error, skip};
        PathKey when is_list(PathKey) ->
            case arweave_throttling_router:lookup_path(Peer, PathKey) of
                {error, _} = E ->
                    %% any error is passed, including unknown_key
                    E;
                {ok, _GroupID} = G ->
                    G
            end
    end.
%% Private
split_path(Path) ->
    case string:split(Path, "/", all) of
        [[]|Rest] ->
            Rest;
        NotEmpty ->
            NotEmpty
    end.

%% @doc Remove variable parameters from paths.
%% Some paths need to have their keys processed to no produce 100s or 1000s
%% of ETS entries.
%% Note: After a few tests, and looking at the endpoints the server (ar_http_iface_server)
%%       provides, the best seems to be just taking the first element from the list usually,
%%       and handle the exceptions (where we skip or likely have different limiting groups depending
%%       on a component after variables).
%%       We also have to keep an eye on the case where the path starts with a hash.
split_path_to_path_key(["tx"|_]) -> {error, skip};
split_path_to_path_key(["block", _Type, _ID, "hash_list"]) -> ["block", "hash_list"];
split_path_to_path_key(["block", _Type, _ID, "wallet_list"]) -> ["block", "wallet_list"];
split_path_to_path_key([First | _])-> [First].
