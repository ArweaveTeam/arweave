---
description: "Arweave build commands and structure"
alwaysApply: true
---

# Arweave Project Rules

## Build Commands
- To compile the project: `./ar-rebar3 prod compile`
- To run tests for a specific module: `./bin/test <module>`
  - Example: `./bin/test ar_data_sync_collect_intervals_test`

## Project Structure
- This is an Erlang/OTP project using rebar3
- Source files: `apps/arweave/src/`
- Test files: `apps/arweave/test/`
- Include files: `apps/arweave/include/`

## Testing Notes
- Test profile uses smaller values for constants like `?PARTITION_SIZE` and `?REPLICA_2_9_ENTROPY_COUNT` (defined in rebar.config)

