
# Arweave Firewall Docs

To support the freedom of individual participants in the network to control what content they store, and to allow the network as a whole to democratically reject content that is widely reviled, the Arweave software provides a blacklisting system.
Each node maintains an (optional) blacklist containing, for example, the hashes or substrings of certain data that it doesn't wish to ever store, and will never include into the block when mining or write to disk the content that matches this.
These blacklists can be built by individuals or collaboratively, or can be imported from other sources.

## Firewall Formats

### Transaction identifiers

To filter out transactions by ID, specify one or more files containing transaction identifiers in the command line, using the `transaction_blacklist` argument. For instance:

```
./arweave-server transaction_blacklist my_tx_blacklist.txt transaction_blacklist my_other_tx_blacklist.txt ...
```

Note that the files have to have the `.txt` extension.

Inside a file, every line is a Base64 encoded transaction identifier. For example:

```
K76dxpFF7MJXa3SPG8XnrgXxf05eAz7jz2Vue1Bdw1M
cPm9Et8pNCh1Boo1aJ7eLGxywhI06O7DQm84V1orBsw
xiQYsaUMtlIq9DvTyucB4gu0BFC-qnFRIDclLv8wUT8
```

### Plain text

Another way to filter out unwanted content is to specify a set of words in plain text. Every word is then tested for the presence in each transaction's data, tags, and receiver address. Specify one or more files enumerating those words using the `content_policy` argument.

```
./arweave-server content_policy my_content_policy.txt content_policy my_other_content_policy.txt ...
```

Note that the files have to have the `.txt` extension.

Inside a file, every line is a word to forbid.

```
damn
ass
butt
```

Do not be too strict as everything containing these words will be censored by your node.

### Hex

You can specify every illicit piece of data using the hexadecimal representation of it. Use the `content_policy` argument (the same as with plain text) to specify the files. The files have to use the `.ndb` extension.

Example of a file:
```
signature:0:*:424144434f4e54454e54
```


### MD5 hashes

You can specify MD5 hashes of the hexadecimal representations of the words using the files with one of the `.hdb`, `.hsb`, `.fp` extensions and the same `content_policy` command line argument.

Example:

```
signature:0:*:7c267e9720076ded59ef7105a38033b1
```