.DEFAULT_GOAL = test_all

DIALYZER = dialyzer
PLT_APPS = erts kernel stdlib sasl inets ssl public_key crypto compiler  mnesia sasl eunit asn1 compiler runtime_tools syntax_tools xmerl edoc tools os_mon

ERL_OPTS= -pa ebin/ \
	-pa lib/prometheus/_build/default/lib/prometheus/ebin \
	-pa lib/accept/_build/default/lib/accept/ebin \
	-pa lib/prometheus_process_collector/_build/default/lib/prometheus_process_collector/ebin \
	-sasl errlog_type error \
	-s prometheus

test_all: test test_apps

test: all
	@erl $(ERL_OPTS) -noshell -s ar test_coverage -s init stop

test_apps: all
	@erl $(ERL_OPTS) -noshell -s ar test_apps -s init stop

test_networks: all
	@erl $(ERL_OPTS) -s ar start -s ar test_networks -s init stop

tnt: test

ct: all
	@ct_run $(ERL_OPTS) -dir test/ -logdir testlog/

no-vlns: test_networks

realistic: all
	@erl $(ERL_OPTS) -noshell -s ar start -s ar_test_sup start realistic

log:
	tail -n 100 -f logs/`ls -t logs |  head -n 1`

catlog:
	cat logs/`ls -t logs | head -n 1`

all: gitmodules build

gitmodules:
	git submodule update --init

build: data blocks hash_lists wallet_lists
	( \
		cd lib/jiffy && \
		../../bin/mute-on-success ./rebar compile && \
		cd ../.. && \
		cp lib/jiffy/priv/jiffy.so ./priv/ \
	)
	(cd lib/prometheus && ./rebar3 compile)
	(cd lib/accept && ./rebar3 compile)
	( \
		cd lib/prometheus_process_collector && \
		./rebar3 compile && \
		cp _build/default/lib/prometheus_process_collector/priv/prometheus_process_collector.so ../../priv/ \
	)
	erlc $(ERLC_OPTS) +export_all -o ebin/ src/ar.erl
	erl $(ERL_OPTS) -noshell -s ar rebuild -s init stop


blocks:
	mkdir -p blocks
	mkdir -p blocks/enc

hash_lists:
	mkdir -p hash_lists

wallet_lists:
	mkdir -p wallet_lists

data:
	mkdir -p data/mnesia

docs: all
	mkdir -p docs
	(cd docs && erl -noshell -s ar docs -pa ../ebin -s init stop)

session: all
	erl $(ERL_OPTS) -s ar start -pa ebin/

sim_realistic: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine realistic

sim_hard: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine hard

clean:
	rm -f ./ebin/*.beam
	rm -f ./logs/*.log
	rm -rf docs
	rm -f priv/jiffy.so priv/prometheus_process_collector.so
	rm -f erl_crash.dump
	(cd lib/jiffy && ./rebar clean)
	(cd lib/prometheus && ./rebar3 clean --all)
	(cd lib/accept && ./rebar3 clean --all)
	(cd lib/prometheus_process_collector && ./rebar3 clean --all)

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
