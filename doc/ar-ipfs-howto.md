# How to set up and run Arweave+IPFS nodes

## ipfs

Download from https://dist.ipfs.io/#go-ipfs

From their website:

> After downloading, untar the archive, and move the ipfs binary somewhere in your executables $PATH using the install.sh script:
>
```
$ tar xvfz go-ipfs.tar.gz
$ cd go-ipfs
$ ./install.sh
```

The install.sh wants to install the ipfs binary to /usr/local/bin.  Rather than run with sudo, I edit the script to change binpaths to, e.g., "/home/ivan/bin" (full path seems to be required).

Set up the local ipfs node with

```
$ ipfs init
```

The ipfs node needs to be running as a daemon before app_ipfs is started.  Start it in a separate terminal (screen/tmux/etc) session with

```
$ ipfs daemon
```

## arweave-server

When running `arweave-server` with the argument `ipfs_pin`, the server listens for incoming TXs with data and an `{"IPFS_Add", Hash}` tag, and `ipfs add`s the data to the local ipfs node.

### ipfs_pin

#### in erlang shell

```erlang
$ arweave-server peer ...

1> app_ipfs:start_pinning().
ok
```

#### with commandline argument

```
$ arweave-server peer ... ipfs_pin
```

### monitoring

Here are some functions for basic monitoring:

At any time, state of the app_ipfs server can be accessed via either of:

```
> app_ipfs:report(app_ipfs).
> app_ipfs:report(IPFSPid).

[{adt_pid,<0.208.0>},          % the simple_adt server, for listening.
 {queue,<0.203.0>},            % the app_queue pid, for sending TXs.
 {wallet,{{<<123,45,67,...},   % used with app_queue to finance sending TXs.
 {ipfs_name,"my_ipfs_node"},   % used to generate the ipfs key and PeerID.
 {ipfs_key,<<"QmXYZ...8jk">>}, % identity of the local ipfs node.
 {blocks,0,[]},                % these last three ...
 {txs,0,[]},                   % ... only used ...
 {ipfs_hashes,0,[]}]           % ... in testing.
```

The status of an IPFS hash can be checked:

```
> app_ipfs:ipfs_hash_status(Hash).

[{pinned, true | false},   % whether the hash is pinned by the local ipfs node
 {tx,     list()      }].  % IDs of TXs containing the ipfs hash & data (generally only one TX)
```
