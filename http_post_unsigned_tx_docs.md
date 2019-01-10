# Internal HTTP API for generating wallets and posting unsigned transactions

## **Warning** only use it if you really really know what you are doing.

The HTTP endpoints are only available if the miner is started with the `enable post_unsigned_tx` CLI option.

## Generate a wallet and receive its secret key

- **URL**
  `/wallet`

- **Method**
  POST

#### Example Response

A key which can be used to sign transactions via `POST /unsigned_tx`.

```javascript
{"wallet_key":"..."}
```

## POST unsigned transaction to the network

Post a transaction to be signed and sent to the network.

- **URL**
  `/unsigned_tx`

- **Method**
  POST

#### Data Parameter (Post body)

```javascript
{
    "last_tx": "",    // base64url encoded ID of last transaction made by address
    "owner": "",      // base64url encoded modulus of wallet making transaction
    "target": "",     // base64url encoded SHA256 hash of recipients modulus
    "quantity": "",   // string representing the amount of sent AR in winston
    "type": "",       // string stating type of transaction 'data' or 'transfer'
    "data": "",       // base64url encoded data being archived in transaction
    "reward": "",     // string representing the mining reward AR in winston
    "wallet_key": ""  // the wallet key (a Base64 encoded string) as returned by the POST /wallet endpoint
}
```

#### Example Response

A transaction ID (Base64 encoded hash of the signature).

```javascript
{"id":"..."}
```