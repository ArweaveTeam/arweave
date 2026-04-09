# Arweave Development

## Building

Use `./ar-rebar3` instead of `./rebar3` for building:
```bash
./ar-rebar3 test compile
```

## Running Tests

```bash
# Run all tests in a module
./bin/test test_module

# Run a specific test
./bin/test test_module:test
```

For example:
```bash
./bin/test ar_unconfirmed_chunk_tests
./bin/test ar_unconfirmed_chunk_tests:get_unconfirmed_chunk_from_disk_pool_test_
```
