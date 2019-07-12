.DEFAULT_GOAL := test_all

TLS_CERT_FILE := priv/tls/cert.pem
TLS_KEY_FILE := priv/tls/key.pem
TLS_FILES := $(TLS_CERT_FILE) $(TLS_KEY_FILE)

DIALYZER := dialyzer
PLT_APPS := erts kernel stdlib sasl inets ssl public_key crypto compiler mnesia sasl eunit asn1 compiler runtime_tools syntax_tools xmerl edoc tools os_mon

ERL_OPTS := -pa ebin/ \
	-pa lib/jiffy/ebin \
	-pa lib/cowboy/ebin \
	-pa lib/cowlib/ebin \
	-pa lib/ranch/ebin \
	-pa lib/prometheus/ebin \
	-pa lib/accept/ebin \
	-pa lib/graphql/ebin \
	-pa lib/prometheus_process_collector/ebin \
	-pa lib/prometheus_httpd/ebin \
	-pa lib/prometheus_cowboy/ebin \
	-sasl errlog_type error \
	-s prometheus

ERL_TEST_OPTS= $(ERL_OPTS) \
	-pa lib/meck/ebin

DEFAULT_PEER_OPTS := \
	peer 188.166.200.45 peer 188.166.192.169 \
	peer 163.47.11.64 peer 159.203.158.108 \
	peer 159.203.49.13 peer 139.59.51.59 \
	peer 138.197.232.192 peer 46.101.67.172

test_all: test test_apps test_ipfs

test: build_test
	@erl $(ERL_TEST_OPTS) -noshell -sname slave -setcookie test -run ar main port 1983 data_dir data_test_slave &
	@erl $(ERL_TEST_OPTS) -noshell -sname master -setcookie test -run ar test_with_coverage -s init stop
	kill 0

test_apps: all
	@erl $(ERL_OPTS) -noshell -sname master -run ar test_apps -s init stop

test_networks: all
	@erl $(ERL_OPTS) -s ar start -s ar test_networks -s init stop

test_ipfs: all
# 	Since we're using mnesia for IPFS, the sname is important.
	@erl $(ERL_OPTS) -noshell -sname master -run ar test_ipfs -s init stop

tnt: test

ct: all
	mkdir -p testlog
	@ct_run $(ERL_OPTS) -dir test/ -logdir testlog/

no-vlns: test_networks

realistic: all
	@erl $(ERL_OPTS) -noshell -s ar start -s ar_test_sup start realistic

log:
	tail -n 100 -f logs/`ls -t logs | grep -v slave | head -n 1`

catlog:
	cat logs/`ls -t logs | grep -v slave | head -n 1`

all: build

build-randomx:
	make -C lib/RandomX
	make -C c_src

gitmodules:
	git submodule foreach 'git remote prune origin' && git submodule sync && git submodule update --init

compile_prod: build-randomx
	./rebar3 compile --deps_only

compile_test: build-randomx
	./rebar3 as test compile

build: gitmodules compile_prod build_arweave

build_test: gitmodules compile_test build_arweave

build_arweave:
	erlc $(ERLC_OPTS) +export_all -o ebin/ src/ar.erl
	erl $(ERL_OPTS) -noshell -s ar rebuild -s init stop

docs: all
	mkdir -p docs
	(cd docs && erl -noshell -s ar docs -pa ../ebin -s init stop)

certs: $(TLS_FILES)
$(TLS_FILES):
	mkdir -p priv/tls
	mkcert \
		-cert-file $(TLS_CERT_FILE) \
		-key-file $(TLS_KEY_FILE) \
		 'gateway.localhost' '*.gateway.localhost'

session: build_test
	erl $(ERL_TEST_OPTS) -noshell -sname slave -setcookie test -run ar main port 1983 data_dir data_test_slave &
	erl $(ERL_TEST_OPTS) -sname master -setcookie test -run ar main data_dir data_test_master
	kill 0

polling_session: all
	erl $(ERL_OPTS) -run ar main polling $(DEFAULT_PEER_OPTS)

polling_gateway_session: all certs
	erl $(ERL_OPTS) -run ar main \
		polling \
		gateway gateway.localhost \
		$(DEFAULT_PEER_OPTS)

sim_realistic: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine realistic

sim_hard: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine hard

clean:
	rm -f ./ebin/*.beam
	rm -rf docs
	rm -f priv/arweave.so priv/jiffy.so priv/prometheus_process_collector.so
	rm -f c_src/ar_mine_randomx.o
	rm -f erl_crash.dump
	rm -f lib/*/ebin/*.beam
	rm -rf lib/*/_build
	rm -rf lib/*/.rebar3
	rm -rf lib/RandomX/obj lib/RandomX/bin
	(cd lib/jiffy && make clean)
	rm -f lib/prometheus/ebin/prometheus.app
	rm -f lib/accept/ebin/accept.app
	rm -f lib/prometheus_process_collector/ebin/prometheus_process_collector.app
	rm -f lib/prometheus_httpd/ebin/prometheus_httpd.app
	rm -f lib/prometheus_cowboy/ebin/prometheus_cowboy.app

todo:
	grep --color --line-number --recursive TODO "src"

docker-image:
	docker build -t arweave .

testnet-docker: docker-image
	cat peers.testnet | sed 's/^/peer /' \
		| xargs docker run --name=arweave-testnet arweave

dev-chain-docker: docker-image
	docker run --cpus=0.5 --rm --name arweave-dev-chain --publish 1984:1984 arweave \
		no_auto_join init mine peer 127.0.0.1:9

build-plt:
	$(DIALYZER) --build_plt --output_plt .arweave.plt \
	--apps $(PLT_APPS)

dialyzer:
	$(DIALYZER) --fullpath  --src -r ./src -r ./lib/*/src ./lib/pss \
	-I ./lib/*/include --plt .arweave.plt --no_native \
	-Werror_handling -Wrace_conditions
