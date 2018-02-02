# Integrating with the Archain

Each node on the Archain network hosts a simple lightweight REST API. This allows developers to communicate with the network via HTTP requests. 

Most programming languages provide 'out of the box' functionality for making web calls and as such can simply communicate using this method.

## Transaction format

Transactions are submitted to the network via this HTTP interface and structured as JSON. 

An example of the transaction format is below:

```javascript
{
	"id": "",
	"last_tx": "",
	"owner": "",
	"target": "",
	"quantity": "",
	"type": "",
	"data": "",
	"reward": "",
	"signature": ""
}
```
**[id]** 
A randomly selected 32-byte identifier base64url encoded.

**[last_tx]** 
The ID of the last transaction made from the same address base64url encoded. If no previous transactions have been made from the address this field is set to an empty string.

**[owner]**
The public exponent of the RSA key pair for the wallet making the transaction base64url encoded.

**[target]**
If making a financial transaction this field contains the wallet address of the recipient base64url encoded. If the transaction is not a financial this field is set to an empty string. 

A wallet address is a SHA256 hash of the raw unencoded RSA public exponent.

**[quantity]**
If making a financial transaction this field contains the amount in Winston to be sent to the receiving wallet. If the transaction is not financial this field is set to the string "0".

(1 AR = 1000000000000 Winston).

**[type]**
This field specifies the type of transaction, transactions can either be 'data' or 'transfer'.

**[data]**
If making an archiving transaction this field contains the data to be archived base64url encoded. If the transaction is not archival this field is set to an empty string.

**[reward]**
This field contains the mining reward for the transaction in Winston.

(1 AR = 1000000000000 Winston).

**[signature]**
The data for the signature is comprised of previous data from the rest of the transaction.

The data to be signed is a concatentation of the raw (entirely unencoded) owner, target, id, data, quantity, reward and last_tx in that order.

The psuedocode below shows this. 

```psuedo
unencode  <- Takes input X and returns the completely unencoded form
sign      <- Takes data D and key K returns a signature of D signed with K

owner     <- unencode(owner)
target    <- unencode(target)
id        <- unencode(id)
data      <- unencode(data)
quantity  <- unencode(quantity)
reward    <- unencode(reward)
last_tx   <- unencode(last_tx)

sig_data <- owner + target + id + data + quantity + reward + last_tx
signature <- sign(sig_data, key)

return signature
```

Once returned the signature is base64url encoded and added to the transactions JSON struct.

The transaction is now complete and ready to be submitted to the network via a POST request on the '/tx' endpoint.

## Example Archiving Transaction

```javascript
{
 	"id": "eDhZfOhEmVZV72h0Xm_AX1MEuPnqvGeeXstBbLY3Sdk",
	"last_tx": "eC8pO0aKOxkQHLLGmfLKvBQnnRlTk1uq10H8fQAATAA",
	"owner": "1Q7Rf...2x0xC",						// Partially omitted due to length
	"target": "",
	"quantity": "0",
	"type": "data",
	"data": "",
	"reward": "2343181818",
	"signature": "Bgb65...7cBR4" 					// Partially omitted due to length 
}
```

## Example Financial Transaction

```javascript
{
  	"id": "iwUl8_2Bc07vOCjE9Q5_VQ8KrvHZu0Rk-eq3c8bF6X8",
	"last_tx": "eDhZfOhEmVZV72h0Xm_AX1MEuPnqvGeeXstBbLY3Sdk",
	"owner": "1Q7Rf...2x0xC",						// Partially omitted due to length
	"target": "f1FHKWEauF3bJYfn6i0mM5zrUfIxn8lUQHlWKUmP04M",
	"quantity": "25550000000000",
	"type": "transfer",
	"data": "",
	"reward": "1907348633",
	"signature": "Hg02G...cdNk8" 					// Partially omitted due to length 
}
```



## Notes

> Please note that in the JSON transaction records all winston value fields (quantity and reward) are strings. This is to allow for interoperability between environments that do not accommodate arbitrary-precision arithmetic. JavaScript for instance stores all numbers as double precision floating point values and as such cannot natively express the integer number of winston. Providing these values as strings allows them to be directly loaded into most 'bignum' libraries.



> Transactions are required to contain the ID of the last transaction made by the same wallet, this value can be retrieved via hitting the '/wallet/[wallet_address]/last_tx' endpoint of any node on the network. More details regarding this endpoint can be found here in the node HTTP interface docs.

