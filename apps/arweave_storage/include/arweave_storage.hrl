-ifndef(ARWEAVE_STORAGE_HRL).
-define(ARWEAVE_STORAGE_HRL, true).

-record(store_info, {
    % Storage module ID, or the default module ID.
    id,
    % Runtime metrics label; undefined before storage starts.
    label,
    % Configured byte range, without overlap.
    configured_range,
    % Byte range including packing-dependent overlap.
    effective_range,
    % Sync byte range; {-1, -1} for the default module.
    padded_range,
    % Directory name, preserving legacy bucket notation.
    disk_dir_name,
    % Storage module directory under the configured data_dir.
    path,
    % Chunk-file directory within path.
    chunk_storage_path,
    % Whether this is an in-place repacking source.
    repack_in_place,
    % Configured packing, not the target of in-place repacking.
    packing,
    % Packing address, or undefined for unpacked data.
    mining_address,
    % Mining difficulty of the configured packing.
    packing_difficulty
}).

-endif.
