#!/bin/bash
echo "-----INSTALLING DEPENDENCIES-----"
sudo apt-get update
sudo apt-get install -y gcc make build-essential autoconf m4 libncurses5-dev libssh-dev unixodbc-dev openjdk-8-jdk libwxgtk3.0-dev xsltproc fop
wget http://www.erlang.org/download/otp_src_20.1.tar.gz
tar -zvxf otp_src_20.1.tar.gz
cd otp_src_20.1
export ERL_TOP=`pwd`
echo "-----BUILDING ERLANG-----"
./configure --without-wx
make -j4
sudo make install
echo "-----INSTALLED ERLANG SUCCESFULLY-----"
cd ../
sudo apt-get install -y git
echo "-----CLONING ARWEAVE REPO-----"
git clone https://github.com/ArweaveTeam/arweave.git arweave && cd arweave && git -c advice.detachedHead=false checkout N.1.1.0
make all
echo "-----FINISHED INSTALLATION-----"