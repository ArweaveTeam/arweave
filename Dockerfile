FROM erlang:20-alpine as builder

RUN apk update && apk add make g++

RUN mkdir /arweave
WORKDIR /arweave

COPY Makefile .
COPY Emakefile .
ADD lib lib
ADD src src
RUN make all

FROM erlang:20-alpine

RUN mkdir /arweave
WORKDIR /arweave

COPY arweave-server .
COPY data data
COPY --from=builder /arweave/ebin ebin

EXPOSE 1984
ENTRYPOINT ["./arweave-server"]
