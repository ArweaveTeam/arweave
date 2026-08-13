# Protocol details and caveats

Non-obvious facts about the Arweave protocol that affect how code in this repo
must be written. Read this before working on mining, packing, or recall-range
code.

## Recall bytes and sub-chunks

Recall bytes always point to chunks, not sub-chunks. All sub-chunks of every
chunk are considered during mining. `Nonce rem SubChunkCount` determines which
sub-chunk goes into the proof.
