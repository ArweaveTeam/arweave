#!/bin/bash
echo "-----INSTALLING DEPENDENCIES-----"
sudo apt-get update
sudo apt-get install -y gcc make build-essential autoconf m4 libncurses5-dev libssh-dev unixodbc-dev openjdk-8-jdk libwxgtk3.0-dev xsltproc fop libsqlite3-dev
wget http://www.erlang.org/download/otp_src_21.3.tar.gz
if [ `shasum -a 256 otp_src_21.3.tar.gz | awk {'print $1'}` != 69a743c4f23b2243e06170b1937558122142e47c8ebe652be143199bfafad6e4 ]; then
	echo 'Integrity check failed for otp_src_21.3.tar.gz. Remove the file and try to download it from another source.'
	exit 1
fi
tar -zvxf otp_src_21.3.tar.gz
cd otp_src_21.3
export ERL_TOP=`pwd`
echo "-----BUILDING ERLANG-----"
./configure --without-wx
make -j4
sudo make install
echo "-----INSTALLED ERLANG SUCCESFULLY-----"
cd ../
sudo apt-get install -y git
echo "-----CLONING ARWEAVE REPO-----"
git clone https://github.com/ArweaveTeam/arweave.git arweave && cd arweave && git -c advice.detachedHead=false checkout stable
make all
echo "-----FINISHED INSTALLATION-----"
