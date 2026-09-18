# arweave_entropy

`arweave_entropy` is the public interface for entropy generation, reuse and
preparation. Internal modules own the entropy cache, per-key generation locks,
generation statistics, footprint slicing and per-store preparation workers.

`arweave_entropy_generation` owns reusable generation and slicing helpers;
`arweave_entropy_preparation` owns per-store preparation scheduling and cursors.

Test builds expose `internal_get_cached/1`, `internal_cache/4` and
`internal_clear_cache/0` through the same public module, so simulators can use
the real cache's weighted entries and
eviction without calling internal modules. These functions are not exported in
production builds; simulation suites start the entropy app to own its tables.

The application root owns the cache tables. The host activates preparation
through `child_spec/0` after packing, storage and device scheduling are ready.
Preparation workers remain under the entropy application's supervisor; the host
lifecycle child stops them before storage and packing shut down.

Generation uses the existing packing workers and their shared RandomX dataset.
It does not start another native worker pool or allocate another dataset.
Host protocol and scheduling calls are isolated in `arweave_entropy_deps`.

`arweave_storage` owns entropy files, preparation cursors and sync records. A
footprint write supplies a fold that slices already-generated binaries on demand,
so it retains the existing single-message write batching without materializing
another full footprint. Storage still serializes entropy and chunk writes under
the same file lock, including when entropy arrives after an unpacked chunk.

When a prepared store is missing entropy, storage reports it to the caller.
`ar_data_sync` generates the replacement outside the storage process, then retries
with `{chunk_with_entropy, Chunk, Entropy}`. Storage rechecks the bucket under its
lock. Unprepared stores still stage unpacked data; syncing does not generate
entropy on demand for those stores.

Cursor filenames, entropy record identifiers, on-disk formats, configuration and
metrics names are unchanged.

Focused tests:

```sh
./bin/ct --dir apps/arweave_entropy/test
```
