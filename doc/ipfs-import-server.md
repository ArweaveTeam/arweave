# IPFS Import Server

## Overview

This server provides a service to registered users who want to store their ipfs hashed data on the Arweave and on Arweave's AR+IPFS nodes.

User registration is not covered in this document, but the result of registration will include an API Key, and a wallet with funds.  The server needs access to the wallet to sign "IPFS_Add" tagged TXs.  The user includes the API Key in API calls (see below).

## API endpoints

### Send a hash to be incentivised

    > POST /api/ipfs/getsend/
    >
    > {
    >  "api_key": <string>,
    >  "ipfs_hash": <string>
    > }
    
    < 200 OK                --  Task queued
    < 208 Already Reported  --  User has already submitted this hash
    < 400 Bad Request       --  invalid json
    < 401 Unauthorized      --  invalid api key


Sending this with a valid API Key (with sufficient funds in wallet) will cause the Arweave server to:
- check request validity & auth (error responses 400, 401)
- check whether User has already called getsend with this hash & if so, return 208
- check whether User status is nofunds, if so return 402
- check User's pending requests > MAX_IPFSAR_PENDING, if so return 429
- check api key ok (& get wallet) & if not, return 401
- queue the task (see below)
- return 200

The server will then (out-of-loop):
- `ipfs get` the hash data and wrap it into an "IPFS-Add" TX
- providing the user has sufficient funds, queue the TX for submission to the Arweave network
- update the user's status with `nofunds` or `queued` as appropriate

Once the TX has been mined, the server will update the user's status again, and charge the user's wallet with the getsend fee.

### Delete a hash request

    > DEL /api/ipfs/<key>/<hash>

If the hash status is `pending` or `nofunds` (see get status below), the hash request is removed.  Otherwise a 400 is returned, with an informative body:

    < 400 "Hash already mined."
    < 400 "Hash already queued."

### Get status of User's requests

    > GET /api/ipfs/<key>/status[/limit/<N>[/offset/<M>]]

    < [
    <   {
    <     "timestamp": "2019-02-07T13:49:23+00:00",
    <     "ipfs_hash": "QmZDQb8iK7BaTAWraCrSDSygZ23UkrhXzwVUuV2ZUKm7Rw",
    <     "status": "queued"
    <   },
    <  ...
    < ]

Returns status of hashes in reverse chronological order.  Limit N and offset M are optional.  Without these arguments, status of all hashes requested by the user is returned.  Otherwise, the specified subset.

The "status" field is one of:

- pending: waiting to be sent to an app-queue
- queued: queued for mining into a TX
- mined: mined into a TX
- nofunds: User ran out of funds before hash was queued

### Get ID(s) of containing TX(s) for a specified IPFS hash

    > GET /api/ipfs/<key>/<ipfs_hash>/tx

    < 404 IPFS hash not found.

    < 200 [<TX.id>]  // If the hash has been mined

    < 200 {
    <     "timestamp": <ISO datetime>,
    <     "ipfs_hash": <ipfs_hash>,
    <     "status": <string>
    <     }  // If the hash has not yet been mined

### Get data for a specified IPFS hash

    > GET /api/ipfs/<ipfs_hash>

    < 404 IPFS hash not found.

    < 200 <TX.data>  // If the hash has been mined

### Get balance of User's wallet

    > GET /api/ipfs/<key>/balance

    < {
    <   "address": "qwe...asd",
    <   "balance": "1165405188938"
    < }

Returns balance (in Winston) in User's wallet, along with the wallet address.

## arweave-server

### command-line parameters

- `ipfs_import` (no arguments): Start the IPFS->AR server.

### shell functions

- `app_ipfs_daemon_server:start()`

Starts the IPFS->AR server (in case it wasn't started at the command-line).

- `app_ipfs_daemon_server:stop()`

Stops ter and all app_queue queues linked to api keys.

- `put_key_wallet_address(APIKey, WalletAddress)`

Loads the wallet keyfile from `wallets/`, starts an app_queue with the wallet, adds all to the mnesia db.

- `put_key_wallet(APIKey, Wallet)`

Starts an app_queue with the wallet, adds all to the mnesia db.

- `get_key_q_wallet(APIKey)`

Returns queue pid and wallet for the key.

- `del_key(APIKey)`

Delete key from the mnesia db.  Stop the associated app_queue.

## client bash script

The shell script [ipfsar.sh](ipfsar.sh) is one possible way of using the service as a client.  When run, the script call `ipfs pin ls` on a local ipfs node, and sends getsend requests to a specified IPFS Import Server.  The script keeps a local ignore list, and can be run from cron or equivalent to automatically request incentivised storage for new hashes.
