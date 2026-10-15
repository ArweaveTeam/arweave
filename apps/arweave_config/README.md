# arweave_config

The node configuration: the option specs, the value store, and loading from
the command line, the environment, and JSON or YAML files. It is a service app.
Every application reads node configuration through it; see
[configuration](../../doc/agents/configuration.md) for adding an option.

## Public interface

- [`arweave_config`](src/arweave_config.erl) is the only public module. It
  reads and writes values (`get/1`, `set/2`), runs the load and runtime modes,
  and covers feature flags, storage-module settings, peer parsing and the
  command-line help.
- Test builds add `internal_*` functions, guarded by `AR_TEST`, that force,
  snapshot and restore the configuration.

## How it works

- **Options.** Each option is a spec map in one of the
  `arweave_config_options_*` modules: its name, default, type, and read and
  write hooks.
- **Load mode.** Startup begins in load mode, where any option can be written.
  `bootstrap/1` loads the environment variables, then either the legacy
  command line and `config.json`, or the long flags and JSON or YAML files.
- **Runtime mode.** Once the node has started, `runtime/0` runs every validator
  once and switches to runtime mode. From then on only options marked
  `runtime => true` accept writes. Each write is validated and rolled back if
  it would make the configuration invalid.
- **Applying changes.** A runtime option's write hook passes the new value to
  the service that uses it, such as the chunk cache or a logging handler.

## External dependencies

| Feature | Application | Description |
|---|---|---|
| Chunk cache | `arweave` ([`ar_chunk_cache`](../arweave/src/ar_chunk_cache.erl)) | Validates and applies the chunk cache size. |
| Repacking | `arweave` ([`ar_repack`](../arweave/src/ar_repack.erl)) | Recomputes repack sizing when its options change. |
| Device locks | `arweave` ([`ar_device_lock`](../arweave/src/ar_device_lock.erl)) | Applies the device-limit and entropy-worker options. |
| Mining server | `arweave` ([`ar_mining_server`](../arweave/src/ar_mining_server.erl)) | Applies the mining cache size. |
| Logging | `arweave` ([`ar_logger`](../arweave/src/ar_logger.erl)) | Starts and stops log handlers. |
| Webhooks | `arweave` ([`ar_webhook`](../arweave/src/ar_webhook.erl)) | Lists the webhook events an option can name. |
| HTTP server | `arweave` ([`ar_http_iface_server`](../arweave/src/ar_http_iface_server.erl)) | Applies connection limits and protocol options. |
| JSON | `arweave` ([`ar_serialize`](../arweave/src/ar_serialize.erl)) | Decodes JSON configuration files. |
| Diagnostics | [`arweave_diagnostic`](../arweave_diagnostic/src/arweave_diagnostic.erl) | Runs every diagnostic report when the node receives `SIGUSR1`. |
| Rate limiters | [`arweave_limiter`](../arweave_limiter/src/arweave_limiter_group.erl) | Applies rate-limiter group settings. |
| Helpers | [`arweave_lib`](../arweave_lib/README.md) | Encoding, decoding and port-parsing helpers, and protocol constants. |

`arweave_config` calls `arweave_lib` directly and reaches every other
application through [`arweave_config_deps`](src/arweave_config_deps.erl). Most
of these calls come from runtime options' write hooks.

## Tests

```sh
./bin/ct --dir apps/arweave_config/test
```
