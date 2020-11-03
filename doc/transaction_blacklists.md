
# Arweave Transaction Blacklists

To support the freedom of individual participants in the network to control what content they store, and to allow the network as a whole to democratically reject content that is widely reviled, the Arweave software provides a blacklisting system.
Each node maintains an (optional) blacklist containing the identifiers of transactions with data it doesn't wish to store.

These blacklists can be built by individuals or collaboratively, or can be imported from other sources.

## Blacklist Sources

### Local Files

Specify one or more files containing transaction identifiers in the command line using the `transaction_blacklist` argument or in a config file via the `transaction_blacklists` field.

```
./bin/start transaction_blacklist my_tx_blacklist.txt transaction_blacklist my_other_tx_blacklist.txt ...
```

Inside a file, every line is a Base64 encoded transaction identifier. For example:

```
K76dxpFF7MJXa3SPG8XnrgXxf05eAz7jz2Vue1Bdw1M
cPm9Et8pNCh1Boo1aJ7eLGxywhI06O7DQm84V1orBsw
xiQYsaUMtlIq9DvTyucB4gu0BFC-qnFRIDclLv8wUT8
```

### HTTP Endpoints

Specify one more HTTP endpoints in the command line, using the `transaction_blacklist_url` argument or in a config file via the `transaction_blacklist_urls` field.

```
./bin/start transaction_blacklist_url http://blacklist.org/blacklist
```

A GET request to a given endpoint has to return a list of transaction identifiers in the
same format the blacklist files use.

## Update Content Policy On The Fly

You can add a new entry to one of your transaction blacklists and the miner will automatically pick up the changes. The blacklisted identifiers are re-fetched from local files and remote endpoints every hour.

If you wish to add more files or remote endpoints, restart the miner with the additional command argument(s) or config parameter(s) specifying the file(s).

The blacklist is persisted on disk. If you restart the node without specifying any files or URLs,
the previously constructed blacklist will take effect. If you want to reset the blacklist,
stop the miner, erase the `ar_tx_blacklist` folder in the data directory, and start the miner.

If you want to whitelist particular transactions, put them into one or more files in the same format
used in blacklist files and specify them on startup via the `transaction_whitelist` command argument
or via the `transaction_whitelists` field in the configuration file.

The whitelist is not persisted so if you restart the node without specifying any whitelists, the
previously whitelisted transactions can be blacklisted again.

## Clean Up Old Data

Data already stored at the time a new transaction is blacklisted is removed automatically.
