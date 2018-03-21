# Arweave Server

This repository holds the Arweave server prototype, as well as the App Developer
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

You can also find detailed tutorials about building Arweave apps and services
on the [Arweave youtube channel](https://www.youtube.com/channel/UCdM3INQ5NAsNjWU8-2y_xjw).

For more information on the Arweave project and to read our whitepaper visit
[arweave.org](https://www.arweave.org/).

Caution: Arweave is in active development. Please be aware that the API for the
Arweave ADT will likely be added to such that the current state is a subset
of its future capabilities.

# Ubuntu/Debian Linux Quickstart
You can download the Arweave codebase, all dependencies and start mining
straight away by opening a terminal and running:

`curl https://gist.githubusercontent.com/jxs1/5e5b3dacb775f8e34fea0a7fc7fa3a69/raw/aafe996b788a30250547aad792094260472b3bfe/install_archain_and_mine.sh | bash && cd archain && ./archain-server peer XXX.XXX.XXX.XXX mine`

Don’t forget to change xxx.xxx.xxx.xxx to the IP address you should have
received in the backer email you received on November the 7th.

# Windows Quickstart
1) Install Erlang and dependancies - http://erlang.org/doc/installation_guide/INSTALL-WIN32.html
2) If not added by default, add Erlang to your PATH
	2.0) WARNING: Changing your PATH variable can be dangerous. Backup your PATH before any changes and proceed carefully.
	2.1) In Search, search for and then select: System (Control Panel)
	2.2) Click the Advanced system settings link.
	2.3) Click Environment Variables. In the section System Variables, find the PATH environment variable and select it. Click Edit. If the PATH environment variable does not exist, click New.
	2.4) Add erl.exe to the PATH
3) Download the most recent version of Arweave from https://github.com/ArweaveTeam/arweave/archive/master.zip
4) Unzip Arweave to a location of your choice
4) Run arweave-server.cmd

# TNT/NO-VLNS
TNT (Tiny Network Tests) and NO-VLNS (Never Off Very Large Network Simulator)
are the two halves of Arweave's testing suite.

You can launch TNT by running `make tnt` and NO-VLNS by running `make no-vlns`.

More information on [TNT](https://medium.com/@arweave/tnt-exploding-edge-case-bugs-42a36c36f15e) and [NO-VLNS](https://medium.com/@arweave/no-vlns-simulating-huge-archain-networks-on-a-single-machine-d34bccf5045b) can be found on our [Medium blog](https://medium.com/@arweave).

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
