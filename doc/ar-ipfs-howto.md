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

The install.sh wants to install the ipfs binary to /usr/local/bin.  Rather than run with sudo, I edit the script to change binpaths to "/home/ivan/bin" (full path seems to be required).

Set up the local ipfs node with

```
$ ipfs init
```

The ipfs node needs to be running as a daemon before app_ipfs is started.  Start it in a separate terminal (screen/tmux/etc) session with

```
$ ipfs daemon
```

## arweave-server

Checkout and build from HEAD of branch `feature/ipfs-wrapper-on-bulk-uploader`.

There are two modes for app_ipfs: read-only (which listens for incoming TXs with data and an `{"IPFS_Add", Hash}` tag, and `ipfs add`s the data), and read-write (which as well as listening, can send tagged TXs to the Arweave network).  Read-write mode requires a wallet with sufficient AR.

I hope to write arweave-server commandline arguments for the above this morning but, in the meantime (and in case, ...) below is how to start these up from the erlang shell.

### read-only

#### in erlang shell

```erlang
$ arweave-server peer ... mine

1> app_ipfs:start_recv_only().
ok
```

#### TODO with commandline argument

```
$ arweave-server peer ... mine ipfs-ro
```

### read-write

#### in erlang shell

```erlang
$ arweave-server peer ... mine

1> ARNode = whereis(http_entrypoint_node).
<0.124.0>

2> Wallet = ar_wallet:load_keyfile("my_arweave_keyfile_abc123XYZ.json").
{{<<123,45,67,...}

3> Name = "my_ipfs_node".

4> {ok, IPFSPid} = app_ipfs:start([ARNode], Wallet, Name).
{ok,<0.207.0>}
```

To load ipfs hashes from a file:

```erlang
> app_ipfs:bulk_get_and_send_from_file(app_ipfs, Filename).
```

The file should be an erlang file with one ipfs per line, represented as an erlang binary.  For example:

```
<<"QmUKZBoJ8ac1oLDz11UQM5c8TTyNvEUVXxUvw773Rc25Gp">>.
<<"QmVMWhsdRZeM1aJGT27n5N3fwzaJmRUmQA1meBbFEWEvLg">>.
<<"QmVXuNGnyKVzp9quoMB2MieQA2ofAryzZbeAsSDzfCDMzv">>.
...
```

(On the one hand adding all those `<<"` and `">>.` is a drag (made less so with awk); on the other hand `file:consult` is a lot nicer than `file:read`).

#### TODO with commandline argument

Will require a wallet with sufficient AR to upload data.

If present will load ipfs hashes from a file in the current directory called ipfs-hashes.erl.

To run:

```
$ arweave-server peer ... mine ipfs-rw load_mining_key <wallet-filename>
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
> ipfs_hash_status(Hash).

[{pinned, true | false},   % whether the hash is pinned by the local ipfs node
 {tx,     list()      }].  % IDs of TXs containing the ipfs hash & data (generally only one TX)
```

## app_ipfs todo

- start ipfs daemon from erlang on app_ipfs:start.
- fail politely on start if ipfs daemon isn't running.
