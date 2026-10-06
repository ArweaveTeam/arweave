%%% @doc Selects the host modules that arweave_config calls, one function per
%%% dependency. Call sites use ?DEP(Name) from include/arweave_config_deps.hrl.
-module(arweave_config_deps).

-export([
    chunk_cache/0,
    repack/0,
    device_lock/0,
    mining_server/0,
    logger/0,
    webhooks/0,
    http_server/0,
    serialize/0,
    diagnostic/0,
    limiter_group/0
]).

chunk_cache() -> ar_chunk_cache.
repack() -> ar_repack.
device_lock() -> ar_device_lock.
mining_server() -> ar_mining_server.
logger() -> ar_logger.
webhooks() -> ar_webhook.
http_server() -> ar_http_iface_server.
serialize() -> ar_serialize.
diagnostic() -> arweave_diagnostic.
limiter_group() -> arweave_limiter_group.
