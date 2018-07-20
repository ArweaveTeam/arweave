# Running Arweave in a Docker Container

## Introduction

Beside running **Arweave** as a native application on a Linux, macOS, or Windows it's also possible to operate it inside a Docker container. The system provides two scripts to build the local container and to run the image. Blocks, data, transactions, and wallets are saved inside Docker volumes. So in case of possibly needed image stops and removals no data is lost.

## Installation

First retrieve an actual copy of **Arweave**. You don't have to care for a stable version, this is automatically done by the build script.

```shell
$ git clone git@github.com:ArweaveTeam/arweave.git
...
$ cd arweave
```

Inside the `bin` directory you'll find two scripts for the work with Docker:

- `docker-build-arweave` to build the **Arweave** image.
- `docker-run-arweave` to run the **Arweave** image.

To build the **Arweave** image simply start the first script without any arguments.

```shell
$ bin/docker-build-arweave
```

First run needs some more time, possible later re-builds depend on the changes but normally are pretty fast.

To let the Docker image work you first need a *mining address* (see according process). It can be passed to the container as argument or as environment variable. So the variants are

```script
# Run image and pass mining address as argument.
$ bin/docker-run-arweave <mining-address>

# Set environment variable and run image.
$ export MINING_ADDR=<mining-addr>
$ bin/docker-run-arweave

# Set environment variable and run image in one line.
$ MINING_ADDR=<mining-addr> & bin/docker-run-arweave
```

During the start the scripts checks for five Docker volumes and creates them if needed:

- `arweave-blocks`
- `arweave-txs`
- `arweave-wallets`
- `arweave-data`
- `arweave-logs`

They contain the according files for blocks, transactions, wallets, management data, and logging informations. Their content is kept between different starts and stops of the container as well as removals and re-builds with a new start.

## Operation

When wanting to check processes, disk usage, or logfile tails a simple login to the container is possible by doing

```shell
$ sudo docker exec -i -t arweave /bin/bash
```

The container itself can be stopped and started with the standard Docker commands

```shell
$ sudo docker stop arweave
$ sudo docker start arweave
```

In case of an update the container has to be stopped and removed

```shell
$ sudo docker stop arweave
$ sudo docker rm arweave
```

Now a simple

```shell
$ bin/docker-build-arweave
```

builds a new image with the newest stable release and it can be started by

```script
$ bin/docker-run-arweave <mining-address>
```

again. Due to the volumes no data is lost.

Enjoy.