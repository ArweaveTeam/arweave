# Client scripts

Command-line scripts that talk to an Arweave node with
[arweave-js](https://github.com/ArweaveTeam/arweave-js). Install once, then
run with node 20 or later:

```sh
cd scripts/client && npm install
```

## post-tx.js

Builds, signs and posts a transaction, then polls its status. The node's
own network name (`GET /info`) is sent as the `X-Network` header, so the
script works against any network the node belongs to.

```sh
node scripts/client/post-tx.js --node http://host:1984 --wallet wallet.json \
    --data "hello" --tag App=post-tx

node scripts/client/post-tx.js --node http://host:1984 --wallet wallet.json \
    --quantity 1000000000000 --target <address>
```

The data is uploaded in chunks after the transaction. A rejected
transaction's error codes are printed from `GET /tx/<id>`.

After posting the script prints `TX=<id>` for the shell and then polls
`GET /tx/<id>/status` every 10 s, `--poll` times, on the posting node and
on every `--watch` node (comma-separated URLs), one line per node.

Options: `--data` or `--data-file`, `--quantity` in winston (default 0),
`--target` (default empty), `--tag k=v`, `--poll`, `--json`; `--node` and
`--wallet` also come from `ARWEAVE_NODE` and `ARWEAVE_WALLET`.
