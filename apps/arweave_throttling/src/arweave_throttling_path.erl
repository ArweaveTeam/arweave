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
%% We make decisions on
split_path_to_path_key(["tx"|_]) -> {error, skip};
split_path_to_path_key(["chunk" | _] ) -> ["chunk"];
split_path_to_path_key(["chunk2" | _]) -> ["chunk"];
split_path_to_path_key(["data_sync_record" | _]) -> ["data_sync_record"];
split_path_to_path_key(["recent_hash_list_diff" | _]) -> ["recent_hash_list_diff"];
split_path_to_path_key(["block", _Type, _ID, "hash_list"]) -> ["block", "hash_list"];
split_path_to_path_key(["block", _Type, _ID, "wallet_list"]) -> ["block", "wallet_list"];
split_path_to_path_key(["metrics" | _ ])-> ["metrics"];
split_path_to_path_key(Other) -> Other. %% TODO: we have to make sure these won't have variables
