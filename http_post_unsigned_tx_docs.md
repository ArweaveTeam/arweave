# Internal HTTP API for generating wallets and posting unsigned transactions

## **Warning** only use it if you really really know what you are doing.

The HTTP endpoints are only available if `arweave-server` is started with the `enable post_unsigned_tx` argument.

## Generate a wallet and receive its secret key

- **URL**
  `/wallet`

- **Method**
  POST

#### Example Response

A key which can be used to sign transactions via `POST /unsigned_tx`.

```javascript
{"wallet_key":"UEhkVh0LBqfIj60-EB-yaDSrMpR2_EytWrY0bGJc_AZaiITJ4PrzRZ_xaEH5KBD4"}
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
    "type": "",       // String stating type of transaction 'data' or 'transfer'.
    "last_tx": "",    // Base64 encoded ID of the last transaction made by this wallet.
    "owner": "",      // Base64 encoded SHA256 hash of the public key making this transaction.
    "target": "",     // Base64 encoded SHA256 hash of recipient's public key. Empty for data transactions.
    "quantity": "",   // Decimal string representation of the amount of sent AR in winston. Empty for data transactions.
    "data": "",       // The base64 encoded data being store in the transaction. Empty for transfer transactions.
    "reward": "",     // Decimal string representation of the mining reward AR amount in winston.
    "wallet_key": ""  // The wallet key as returned by the POST /wallet endpoint.
}
```


#### Example Response

A transaction ID (Base64 encoded hash of the signature).

```javascript
{"id": "F8ITA-zojpRtUNnULnKasJCHL46rcqQBpSyqBekWnF30S7GCd58LcIcOXhYnYL6U"}
```
