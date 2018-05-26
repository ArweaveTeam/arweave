FROM erlang:20-alpine

RUN apk update && apk add make g++

RUN mkdir /arweave
WORKDIR /arweave

COPY Makefile .
COPY Emakefile .
COPY arweave-server .
ADD data data
ADD lib lib
ADD src src
RUN make all

EXPOSE 1984

ENTRYPOINT ["./arweave-server"]
