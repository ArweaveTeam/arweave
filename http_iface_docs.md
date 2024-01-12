# Arweave HTTP Interface Documentation

You can run this HTTP interface using Postman [![Postman](https://run.pstmn.io/button.svg)](https://app.getpostman.com/run-collection/8af0090f2db84e979b69) or you can find the documentation [here](https://documenter.getpostman.com/view/5500657/RWgm2g1r).


## GET network information

Retrieve the current network information from a specific node.

- **URL**
  `/info`
- **Method**
  GET

#### Example Response

A JSON array containing the network information for the current node.

```javascript
{
  "network": "arweave.N.1",
  "version": "3",
  "height": "2956",
  "blocks": "3495",
  "peers": "12"
}
```

#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/info';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```





## GET full transaction via ID

Retrieve a JSON transaction record via the specified ID.

- **URL**
  `/tx/[transaction_id]`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64 encoded ID associated with the transaction


#### Example Response

A JSON transaction record.

```javascript
{
  "id": "VvNF3aLS28MXD_o4Lv0lF9_WcxMibFOp166qDqC1Hlw",
  "last_tx": "bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is",
  "owner": "1Q7RfP...J2x0xc",
  "tags": [],
  "target": "",
  "quantity": "0",
  "data": "3DduMPkwLkE0LjIxM9o",
  "reward": "1966476441",
  "signature": "RwBICn...Rxqi54"
}
```


## GET additional info about the transaction via ID

- **URL**
  `/tx/[transaction_id]/status`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : base64url encoded ID associated with the transaction


#### Example Response

```javascript
{"block_indep_hash": "KCdtB29b5V0rz2hX_sSGfEd5Fw7iTEiuXp5M34dWPEIdhxPqf3rsNyRFUznAhDzb","block_height":10,"number_of_confirmations":3}
```
#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/tx/VvNF3aLS28MXD_o4Lv0lF9_WcxMibFOp166qDqC1Hlw';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```





## GET specific transaction fields via ID

Retrieve a string of the requested field for a given transaction.

- **URL**
  `/tx/[transaction_id]/[field]`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64url encoded ID associated with the transaction
  [field] : A string containing the name of the data field being requested
- **Fields**
  id | last_tx | owner | target | quantity | data | reward | signature


#### Example Response

A string containing the requested field.

```javascript
"bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/tx/VvNF3aLS28MXD_o4Lv0lF9_WcxMibFOp166qDqC1Hlw/last_tx';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```






## GET transaction body as HTML via ID

Retrieve the data segment of the transaction body decoded from base64url encoding.
If the transaction was an archived website then the result will be browser rendererable HTML.

- **URL**
  `/tx/[transaction_id]/data.html`
- **Method**
  GET
- **URL Parameters**
  [transaction_id] : Base64url encoded ID associated with the transaction


#### Example Response

A string containing the requested field.

```javascript
"Hello World"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/tx/B7j_bkDICQyl_y_hBM68zS6-p8-XiFCUmEBaXRroFTM/data.html'
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```





## GET estimated transaction price

Returns an estimated cost for a transaction of the given size.
The returned amount is in winston (the smallest division of AR, 1 AR = 1000000000000 winston).

The endpoint is pessimistic, it reports the price as if the network difficulty was smaller by one, to account for the possible difficulty change.

- **URL**
  `/price/[byte_size]`
- **Method**
  GET
- **URL Parameters**
  [byte_size] : The size of the transaction's data field in bytes. For financial transactions without associated data, this should be zero.


#### Example Response

A string containing the estimated cost of the transaction in Winston.

```javascript
"1896296296"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/price/2048';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```






## GET block via ID

Retrieve a JSON array representing the contents of the block specified via the ID.

- **URL**
  `/block/hash/[block_id]`
- **Method**
  GET
- **URL Parameters**
  [block_id] : Base64url encoded ID associated with the block


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "c7V-8dLmmqo",
  "previous_block": "yeCiFpWcguWtWRJnJ_XOKhQXw6xtiOHh-rAw-RjX0YE",
  "timestamp": 1517563547,
  "last_retarget": 1517563547,
  "diff": 8,
  "height": 30,
  "hash": "-3-oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "indep_hash": "oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "unclaimed"
}
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/block/hash/oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs';//Use "indep_hash" above,not hash
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```







## GET block via height

Retrieve a JSON array representing the contents of the block specified via the block height.

- **URL**
  `/block/height/[block_height]
- **Method**
  GET
- **URL Parameters**
  [block_height] : The height at which the block is being requested for


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "c7V-8dLmmqo",
  "previous_block": "yeCiFpWcguWtWRJnJ_XOKhQXw6xtiOHh-rAw-RjX0YE",
  "timestamp": 1517563547,
  "last_retarget": 1517563547,
  "diff": 8,
  "height": 30,
  "hash": "-3-oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "indep_hash": "oyxTcYAgbbNoFyDz8hqs7KCJHI4qb4VdER9Jotbs",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "unclaimed"
}
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/block/height/1101';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```








## GET current block

Retrieve a JSON array representing the contents of the current block, the network head.

- **URL**
  `/current_block`
- **Method**
  GET


#### Example Response

A JSON array detailing the block.

```javascript
{
  "nonce": "rihlezm7XAc",
  "previous_block": "pc-0MvV6lQOWt0O2L3VcSheOfIdymntOBVcloERVbQQ",
  "timestamp": 1517564276,
  "last_retarget": 1517564044,
  "diff": 24,
  "height": 166,
  "hash": "mGe34a3DcT8HLE0BfaME38XUelENSjPQA-vcYJG6PGs",
  "indep_hash": "ntoWN8DMFSuxPsdF8CelZqP03Gr4GahMBXX8ZkyPA3U",
  "txs": [...],
  "hash_list": [...],
  "wallet_list": [...],
  "reward_addr": "unclaimed"
}
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/current_block';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```







## GET wallet balance via address

Retrieve the balance of the wallet specified via the address.
The returned amount is in winston (the smallest division of AR, 1 AR = 1000000000000 winston).

- **URL**
  `/wallet/[wallet_address]/balance`
- **Method**
  GET
- **URL Parameters**
  [wallet_address] : A base64url encoded SHA256 hash of the raw RSA modulus.


#### Example Response

A string containing the balance of the wallet.

```javascript
"1249611338095239"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/wallet/VukPk7P3qXAS2Q76ejTwC6Y_U_bMl_z6mgLvgSUJIzE/balance';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```








## GET last transaction via address

Retrieve the ID of the last transaction made by the given address.

- **URL**
  `/wallet/[wallet_address]/last_tx`
- **Method**
  GET
- **URL Parameters**
  [wallet_address] : A Base64 encoded SHA256 hash of the public key.


#### Example Response

A string containing the ID of the last transaction made by the given address.

```javascript
"bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is"
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/wallet/VukPk7P3qXAS2Q76ejTwC6Y_U_bMl_z6mgLvgSUJIzE/last_tx';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## GET transactions made by the wallet

Retrieve identifiers of transactions made by the given wallet.

- **URL**
  `/wallet/[wallet_address]/txs/[earliest_tx]`
- **Method**
  GET
- **URL Parameters**

  - [wallet_address] : A Base64 encoded SHA256 hash of the public key.
  - [earliest_tx] (optional) : A Base64 encoded ID of the earliest transaction to fetch. If not specified, all transactions made by the given wallet are returned.


#### Example Response

A JSON list of base64url encoded transaction identifiers.

```javascript
["bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is","b23...xg"]
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/wallet/VukPk7P3qXAS2Q76ejTwC6Y_U_bMl_z6mgLvgSUJIzE/txs/bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```


## Get transactions sent to the given address

Retrieve identifiers of transfer transactions depositing to the given wallet. The index is partial - only transactions known by the given node are returned.

- **URL**
  `/wallet/[wallet_address]/deposits/[earliest_deposit]`
- **Method**
  GET
- **URL Parameters**

  - [wallet_address] : A Base64 encoded SHA256 hash of the public key.
  - [earliest_deposit] (optional) : A Base64 encoded ID of the earliest transaction to fetch. If not specified, all deposits known by the node are fetched.


#### Example Response

A JSON list of base64url encoded transaction identifiers.

```javascript
["bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is","b23...xg"]
```

## GET nodes peer list  

Retrieve the list of peers held by the contacted node.

- **URL**
  `/peers`
- **Method**
  GET


#### Example Response

A list containing the IP addresses of all of the nodes peers.

```javascript
[
  "127.0.0.1:1985",
  "127.0.0.1.:1986"
]
```


#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/peers';
var url = node + path;
var xhr = new XMLHttpRequest();

xhr.open('GET', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send();
```








## POST transaction to network

Post a transaction to the network.

- **URL**
  `/tx`

- **Method**
  POST


#### Data Parameter (Post body)

```javascript
{
    "last_tx": "",  // Base64 encoded ID of the last transaction made by this wallet. Empty if this is the first transaction.
    "owner": "",    // The public key making this transaction.
    "target": "",   // Base64 encoded SHA256 hash of recipient's public key. Empty for data transactions.
    "quantity": "", // Decimal string representation of the amount of sent AR in winston. Empty for data transactions.
    "data": "",     // The Base64 encoded data being store in the transaction. Empty for transfer transactions.
    "reward": "",   // Decimal string representation of the mining reward AR amount in winston.
    "signature": "" // Base64 encoded signature of the transaction
}
```

#### JavaScript Example Request

```javascript
var node = 'http://127.0.0.1:1984';
var path = '/tx';
var url = node + path;
var xhr = new XMLHttpRequest();
var post =
    {
      "id": "VvNF3aLS28MXD_o4Lv0lF9_WcxMibFOp166qDqC1Hlw",
      "last_tx": "bUfaJN-KKS1LRh_DlJv4ff1gmdbHP4io-J9x7cLY5is",
      "owner": "1Q7RfP...J2x0xc",
      "tags": [],
      "target": "",
      "quantity": "0",
      "data": "3DduMPkwLkE0LjIxM9o",
      "reward": "1966476441",
      "signature": "RwBICn...Rxqi54"
  };

xhr.open('POST', url);
xhr.onreadystatechange = function() {
  if(xhr.readystate == 4 && xhr.status == 200) {
    // Do something.
  }
};
xhr.send(post);
```



> Please note that in the JSON transaction records all winston value fields (quantity and reward) are strings. This is to allow for interoperability between environments that do not accommodate arbitrary-precision arithmetic. JavaScript for instance stores all numbers as double precision floating point values and as such cannot natively express the integer number of winston. Providing these values as strings allows them to be directly loaded into most 'bignum' libraries.





# Contact

If you have questions or comments on the Arweave HTTP interface you can get in touch by
finding us on [Twitter](https://twitter.com/ArweaveTeam/), [Reddit](https://www.reddit.com/r/arweave), [Discord](https://discord.gg/2ZpV8nM) or by emailing us at team@arweave.org.

# License
The Arweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.
