#!/usr/bin/env node
'use strict';
// Build, sign and post a transaction with arweave-js, then poll its status.
//
//   node scripts/client/post-tx.js --node http://host:1984 --wallet wallet.json \
//       [--data "text" | --data-file path] [--quantity 0] [--target addr] \
//       [--tag k=v ...] [--poll 6] [--watch url,url]
//
// The node's own network name (GET /info) is sent as the X-Network header,
// so the same script works against any network. The data is
// uploaded in chunks after the transaction. A rejected transaction's error
// codes are read back from GET /tx/<id>.

const fs = require('node:fs');
const Arweave = require('arweave');

const USAGE = `Usage: node post-tx.js --node <url> --wallet <path> [options]

Options:
  --node <url>          Node base URL, e.g. http://host:1984 (env ARWEAVE_NODE)
  --wallet <path>       JWK key file of the sending wallet (env ARWEAVE_WALLET)
  --data <string>       Data as a UTF-8 string (default none)
  --data-file <path>    Data read from a file instead of --data
  --quantity <winston>  Amount to transfer (default 0)
  --target <address>    Recipient address (default empty)
  --tag <k=v>           Tag; repeat for several
  --poll <n>            Status polls after posting, 10 s apart (default 6)
  --watch <url,...>     Poll the status on these nodes too, one line per node
  --json                Print the transaction JSON
  -h, --help            This help
`;

function die(msg) {
    process.stderr.write(`error: ${msg}\n`);
    process.exit(1);
}

function parseArgs(argv) {
    const opts = {
        node: process.env.ARWEAVE_NODE || null,
        wallet: process.env.ARWEAVE_WALLET || null,
        data: null,
        dataFile: null,
        quantity: '0',
        target: '',
        tags: [],
        poll: 6,
        watch: [],
        json: false,
    };
    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        const value = () => {
            if (i + 1 >= argv.length) die(`${arg} needs a value`);
            return argv[++i];
        };
        switch (arg) {
            case '-h': case '--help': process.stdout.write(USAGE); process.exit(0);
            case '--node': opts.node = value().replace(/\/+$/, ''); break;
            case '--wallet': opts.wallet = value(); break;
            case '--data': opts.data = value(); break;
            case '--data-file': opts.dataFile = value(); break;
            case '--quantity': opts.quantity = value(); break;
            case '--target': opts.target = value(); break;
            case '--tag': {
                const kv = value();
                const eq = kv.indexOf('=');
                if (eq < 1) die(`bad tag "${kv}", expected k=v`);
                opts.tags.push([kv.slice(0, eq), kv.slice(eq + 1)]);
                break;
            }
            case '--poll': opts.poll = Number(value()); break;
            case '--watch': opts.watch = value().split(',').map((u) => u.trim().replace(/\/+$/, '')).filter(Boolean); break;
            case '--json': opts.json = true; break;
            default: die(`unknown option ${arg}\n${USAGE}`);
        }
    }
    if (!opts.node) die('--node is required (or ARWEAVE_NODE)');
    if (!opts.wallet) die('--wallet is required (or ARWEAVE_WALLET)');
    if (!/^\d+$/.test(opts.quantity)) die('--quantity must be an integer in winston');
    if (opts.quantity !== '0' && !opts.target) die('--quantity needs --target');
    if (opts.data !== null && opts.dataFile) die('use --data or --data-file, not both');
    return opts;
}

function client(nodeURL) {
    const url = new URL(nodeURL);
    return Arweave.init({
        host: url.hostname,
        port: Number(url.port) || (url.protocol === 'https:' ? 443 : 80),
        protocol: url.protocol.replace(':', ''),
        timeout: 60000,
    });
}

async function main() {
    const opts = parseArgs(process.argv.slice(2));
    const jwk = JSON.parse(fs.readFileSync(opts.wallet, 'utf8'));
    const data = opts.dataFile ? fs.readFileSync(opts.dataFile)
        : opts.data !== null ? Buffer.from(opts.data, 'utf8') : null;

    // The network name goes into X-Network on every request.
    let arweave = client(opts.node);
    const info = await arweave.network.getInfo();
    arweave = Arweave.init({ ...arweave.api.config, network: info.network });
    const address = await arweave.wallets.jwkToAddress(jwk);
    const balance = await arweave.wallets.getBalance(address);
    console.log(`node ${opts.node} network ${info.network} height ${info.height}`);
    console.log(`from ${address} balance ${arweave.ar.winstonToAr(balance)} AR`);

    const attributes = { quantity: opts.quantity, target: opts.target };
    if (data) attributes.data = data;
    const tx = await arweave.createTransaction(attributes, jwk);
    for (const [k, v] of opts.tags) tx.addTag(k, v);
    await arweave.transactions.sign(tx, jwk);
    if (opts.json) console.log(JSON.stringify(tx.toJSON(), null, 2));

    console.log(`posting ${tx.id} (${data ? data.length : 0} bytes, ` +
        `quantity ${opts.quantity}, fee ${tx.reward} winston)`);
    const res = await arweave.transactions.post(tx);
    if (res.status !== 200) {
        // The node keeps the verification error codes of a rejected transaction
        // for a while and serves them as a 410 on GET /tx/<id>.
        const codes = await arweave.api.get(`tx/${tx.id}`).catch((e) => e.response || {});
        const detail = codes.status === 410 ? ` (${codes.data})` : '';
        die(`POST /tx: ${res.status} ${JSON.stringify(res.data)}${detail}`);
    }
    console.log('accepted');
    console.log(`TX=${tx.id}`);

    // Each poll asks the posting node, then every --watch node, for
    // GET /tx/<id>/status: 202 pending, 200 with the block, 404 unknown.
    const watched = [opts.node, ...opts.watch.filter((u) => u !== opts.node)];
    const statusLine = async (url) => {
        const name = new URL(url).hostname.split('.')[0];
        try {
            const res = await fetch(`${url}/tx/${tx.id}/status`, {
                headers: { 'x-network': info.network }, signal: AbortSignal.timeout(8000),
            });
            const body = (await res.text()).trim();
            let detail = body;
            try {
                const j = JSON.parse(body);
                if (j.block_height) detail = `block ${j.block_height} ${String(j.block_indep_hash).slice(0, 8)} confirmations ${j.number_of_confirmations}`;
            } catch { /* not JSON */ }
            return `  ${name.padEnd(22)} ${res.status} ${detail}`;
        } catch (e) {
            return `  ${name.padEnd(22)} - ${e.name === 'TimeoutError' ? 'timeout' : e.message}`;
        }
    };
    for (let i = 0; i < opts.poll; i++) {
        await new Promise((r) => setTimeout(r, 10000));
        console.log(new Date().toISOString().slice(11, 19) + ' UTC');
        const lines = await Promise.all(watched.map(statusLine));
        for (const l of lines) console.log(l);
        if (opts.watch.length === 0 && lines[0].includes(' 200 ')) break;
    }
}

main().catch((e) => die(e.stack || String(e)));
