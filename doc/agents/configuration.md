# Configuration

All node configuration goes through `arweave_config`. It is the single source of
truth — config file, CLI, validation, and runtime-vs-static gating all live there.

## Reading a value

```erlang
arweave_config:get([network, client, tcp, keepalive])
```

## Adding an option

1. Declare its spec in a contributor module under
   `apps/arweave_config/src/options/arweave_config_options_*.erl`.
2. Register that module in `arweave_config_options_spec:option_modules/0`.

## What must not be used

Do NOT use `application:get_env/2,3`, `application:set_env`, or `sys.config` /
`vm.args` application env to read or define node configuration. If you need a
knob operators can set, it must be an `arweave_config` option.

## What is not configuration

Compile-time constants that are not operator-facing — internal timeouts,
intervals, buffer sizes — stay as module `-define`s. Those are implementation
details, not configuration.

## Operator-facing documentation

`config help` is the canonical option reference. There is no separate
option-reference page to keep in sync; when an option's documentation changes,
it changes in the spec.
