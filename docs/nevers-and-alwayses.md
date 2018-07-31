# Nevers and Alwayses

Things to never do (in the codebase), and things to always do.

## Never

- NEVER make synchronous calls from ar_node to another process.  This would risk locking part of the network.

- NEVER get blocks by height.

## Always

- ALWAYS get blocks by hash.
