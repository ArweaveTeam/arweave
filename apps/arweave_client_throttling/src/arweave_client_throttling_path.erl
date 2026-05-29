-module(arweave_client_throttling_path).

-export([path_to_group_id/1]).

path_to_group_id(Path) ->
    split_path_to_group_id(split_path(Path)).

%% Private
split_path(Path) ->
    case string:split(Path, "/", all) of
        [[]|Rest] ->
            Rest;
        NotEmpty ->
            NotEmpty
    end.

split_path_to_group_id(["tx"|_]) -> skip;
split_path_to_group_id(["chunk" | _]) -> chunk;
split_path_to_group_id(["chunk2" | _]) -> chunk;
split_path_to_group_id(["data_sync_record" | _]) -> data_sync_record;
split_path_to_group_id(["recent_hash_list_diff" | _]) -> recent_hash_list_diff;
split_path_to_group_id(["hash_list"]) -> block_index;
split_path_to_group_id(["hash_list2"]) -> block_index;
split_path_to_group_id(["block_index"]) -> block_index;
split_path_to_group_id(["block_index2"]) -> block_index;
split_path_to_group_id(["block", _Type, _ID, "hash_list"]) -> block_index;
split_path_to_group_id(["wallet_list"]) -> wallet_list;
split_path_to_group_id(["block", _Type, _ID, "wallet_list"]) -> wallet_list;
split_path_to_group_id(["vdf"]) -> get_vdf;
split_path_to_group_id(["vdf2"]) -> get_vdf;
split_path_to_group_id(["vdf", "session"]) -> get_vdf_session;
split_path_to_group_id(["vdf2", "session"]) -> get_vdf_session;
split_path_to_group_id(["vdf3", "session"]) -> get_vdf_session;
split_path_to_group_id(["vdf4", "session"]) -> get_vdf_session;
split_path_to_group_id(["vdf", "previous_session"]) -> get_previous_vdf_session;
split_path_to_group_id(["vdf2", "previous_session"]) -> get_previous_vdf_session;
%% No vdf3 prev_session in ar_blacklist_middleware.hrl ?RPM_BY_PATH
split_path_to_group_id(["vdf4", "previous_session"]) -> get_previous_vdf_session;
split_path_to_group_id(["metrics" | _ ])-> metrics;
split_path_to_group_id(_) -> general.
