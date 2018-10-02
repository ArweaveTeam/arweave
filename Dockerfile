# -----
# Stage 1: Build the Arweave Server
# -----

FROM ubuntu:18.04 as builder

RUN apt-get update
RUN apt-get install --no-install-recommends --no-install-suggests -y \
		apt-utils \
		make \
		g++ \
		ca-certificates \
		git \
		curl \
		erlang
RUN rm -rf /var/lib/apt/lists/*

RUN mkdir /arweave
WORKDIR /arweave

RUN git clone https://github.com/ArweaveTeam/arweave.git . \
		&& git -c advice.detachedHead=false checkout stable \
		&& git submodule update --init

# E.g. "-DTARGET_TIME=5 -DRETARGET_BLOCKS=10" or "-DFIXED_DIFF=2"
ARG ERLC_OPTS

RUN make build

# -----
# Stage 2: Arweave Server Runtime
# -----

FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install --no-install-recommends --no-install-suggests -y \
		apt-utils \
		coreutils \
		erlang
RUN rm -rf /var/lib/apt/lists/*

RUN mkdir /arweave
WORKDIR /arweave

COPY --from=builder /arweave/docker-arweave-server .
COPY --from=builder /arweave/data data
COPY --from=builder /arweave/priv priv
COPY --from=builder /arweave/ebin ebin
COPY --from=builder /arweave/src/av/sigs src/av/sigs
COPY --from=builder /arweave/lib/prometheus/_build/default/lib/prometheus/ebin \
		lib/prometheus/_build/default/lib/prometheus/ebin
COPY --from=builder /arweave/lib/accept/_build/default/lib/accept/ebin \
		lib/accept/_build/default/lib/accept/ebin
COPY --from=builder /arweave/lib/prometheus_process_collector/_build/default/lib/prometheus_process_collector/ebin \
		lib/prometheus_process_collector/_build/default/lib/prometheus_process_collector/ebin
COPY --from=builder /arweave/lib/prometheus_process_collector/_build/default/lib/prometheus_process_collector/priv \
		lib/prometheus_process_collector/_build/default/lib/prometheus_process_collector/priv

EXPOSE 1984
ENTRYPOINT ["./docker-arweave-server"]

# -----
# EOF
# -----
