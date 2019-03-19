# Arweave Server

This repository holds the Arweave server, as well as the App Developer
Toolkit (ADT) and the Arweave testing framework, TNT/NO-VLNS.

Arweave is a distributed, cryptographically verified permanent archive built
on a cryptocurrency that aims to, for the first time, provide feasible data
permanence. By leveraging our novel Blockweave datastructure, data is stored
in a decentralised, peer-to-peer manner where miners are incentivised to
store rare data.

# Requirements

In order to run the Arweave server prototype and ADT, a recent (R20 and above)
version of Erlang/OTP is required as well as a version of make.

# Getting Started
To get started, simply download this repo. You can start an Arweave server
session simply by running `make session`.

You can learn more about building Arweave ADT apps by checking out our
documentation [here](ADT_README.md).

For more information on the Arweave project and to read our lightpaper visit
[arweave.org](https://www.arweave.org/).

# Mining

To start mining, follow the instructions in the [mining guide](https://docs.arweave.org/info/mining/mining-guide).

# Development

## Prerequisites

- Erlang OTP v20
- GNU Make

## Run tests

```
make test
```

## Run distributed tests

```
make distributed-test
```

## Build and enter shell

```
make session
```

## Build in the distributed mode and enter the master shell

```
make distributed-session
```

# App Developer Toolkit (ADT)
You can find separate documentation for the App Developer Toolkit [here](ADT_README.md).

# HTTP API
You can find documentation regarding our HTTP interface [here](http_iface_docs.md).

# Contact
If you have questions or comments on the Arweave you can get in touch by
finding us on [Twitter](https://twitter.com/ArweaveTeam/), [Reddit](https://www.reddit.com/r/arweave), [Discord](https://discord.gg/2ZpV8nM) or by
emailing us at team@arweave.org.

# License
The Arweave project is released under GNU General Public License v2.0.
See [LICENSE](LICENSE.md) for full license conditions.
