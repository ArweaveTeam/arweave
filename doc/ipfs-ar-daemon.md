# AR+IPFS daemon

## Overview

User is running an ipfs node.

User registers with Arweave:
- Arweave creates a wallet for User
- User pays to fund wallet or Arweave funds wallet
- User is sent an API Key which:
  - authorises use of Arweave's ar+ipfs api
  - links to their wallet
- User is sent bash script ipfsar.sh (with user's guide) to run as a daemon

## bash script

see `ipfsar.sh`

Bash script will already have User's API Key, and an Arweave server IP address.

When run, script will do the following:
- call `ipfs pin ls` & convert output to a list of ipfs hashes
- with each hash:
  - if not in local ignore list
    - curl the Arweave endpoint below
    - on 2xx response: add hash to local ignore list

### TODO periodic execution
script currently relies on cron (or equivalent) for periodic execution.  Make self-sufficient.

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

task queued:
- ipfs get the hash data
- wrap the data into an "IPFS-Add" TX
- estimate cost of TX
- if not sufficient funds in wallet linked to User's API Key: add nofunds to User's status
- TX is funded from wallet linked to User's API Key
- submit to the Arweave network
- Once TX has been mined:
  - add hash to ignore list
  - charge wallet with get-send fee

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
    <   }, ...]

Returns status of hashes in reverse chronological order.  Limit N and offset M are optional.  Without these arguments, status of all hashes requested by the user is returned.  Otherwise, the specified subset.

Field "status" is one of:

- pending: waiting to be sent to an app-queue
- queued: queued for mining into a TX
- mined: mined into a TX
- nofunds: User ran out of funds before hash was queued

### Get ID of containing TX for a specified IPFS hash

    > GET /api/ipfs/<key>/<ipfs_hash>

    < 404 IPFS hash not found.

    < 200 <TX.id>  // If the hash has been mined

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
    <   "address": "gIK2HLIhvFUoAJFcpHOqwmGeZPgVZLcE3ss8sT64gFY",
    <   "balance": "1165405188938"
    < }

Returns balance (in Winston) in User's wallet, along with the wallet address.

## arweave-server

### command-line parameters

- `ipfs_import` (no arguments): Start the IPFS->AR server.

Starts the app_ipfs_daemon_server:

- initialised the mnesia db is necessary
- starts the cleaner_upper process (updates user hash statuses)

### shell functions

- `app_ipfs_daemon_server:start()`

Starts the IPFS->AR server (in case it wasn't started at the command-line).

- `app_ipfs_daemon_server:stop()`

Stops all app_queue queues linked to api keys.

- `put_key_wallet_address(APIKey, WalletAddress)`

Loads the wallet keyfile from wallets/, starts an app_queue with the wallet, adds all to the mnesia db.

- `put_key_wallet(APIKey, Wallet)`

Starts an app_queue with the wallet, adds all to the mnesia db.

- `get_key_q_wallet(APIKey)`

Returns queue pid and wallet for the key.

- `del_key(APIKey)`

Delete key from the mnesia db.  Stop the associated app_queue.
