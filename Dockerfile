FROM erlang:22.2.4

LABEL maintainer="Arweave Team <hello@arweave.org>"

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app
COPY . .

RUN apt-get update
RUN apt-get install apt-utils -y
RUN apt-get install build-essential -y
RUN apt-get install g++ clang gcc -y
RUN apt-get install git -y
RUN apt-get install cmake -y
RUN apt-get install make -y
RUN apt-get install libsqlite3-dev -y
RUN apt-get install erlang-dev -y

RUN git submodule init
RUN git submodule update

RUN ./rebar3 as prod tar
RUN ulimit -n 10240

CMD ["/bin/bash"]