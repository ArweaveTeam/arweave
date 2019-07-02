.DEFAULT_GOAL = test_all

DIALYZER = dialyzer
PLT_APPS = erts kernel stdlib sasl inets ssl public_key crypto compiler mnesia sasl eunit asn1 compiler runtime_tools syntax_tools xmerl edoc tools os_mon

ERL_OPTS= -pa ebin/ \
	-pa lib/jiffy/ebin \
	-pa lib/cowboy/ebin \
	-pa lib/cowlib/ebin \
	-pa lib/ranch/ebin \
	-pa lib/prometheus/ebin \
	-pa lib/accept/ebin \
	-pa lib/prometheus_process_collector/ebin \
	-pa lib/prometheus_httpd/ebin \
	-pa lib/prometheus_cowboy/ebin \
	-sasl errlog_type error \
	-s prometheus

test_all: test test_apps

test: all
	@erl $(ERL_OPTS) -noshell -sname slave -setcookie test -run ar main port 1983 data_dir data_test_slave -pa ebin/ &
	@erl $(ERL_OPTS) -noshell -sname master -setcookie test -run ar test_with_coverage -s init stop

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
	tail -n 100 -f logs/`ls -t logs | grep -v slave | head -n 1`

catlog:
	cat logs/`ls -t logs | head -n 1`

all: gitmodules build

gitmodules:
	git submodule update --init

build:
	./rebar3 compile --deps_only
	erlc $(ERLC_OPTS) +export_all -o ebin/ src/ar.erl
	erl $(ERL_OPTS) -noshell -s ar rebuild -s init stop

docs: all
	mkdir -p docs
	(cd docs && erl -noshell -s ar docs -pa ../ebin -s init stop)

session: all
	erl $(ERL_OPTS) -noshell -sname slave -setcookie test -run ar main port 1983 data_dir data_test_slave -pa ebin/ &
	erl $(ERL_OPTS) -sname master -setcookie test -run ar main data_dir data_test_master -pa ebin/

sim_realistic: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine realistic

sim_hard: all
	erl $(ERL_OPTS) -s ar_network spawn_and_mine hard

clean:
	rm -f ./ebin/*.beam
	rm -rf docs
	rm -f priv/jiffy.so priv/prometheus_process_collector.so
	rm -f erl_crash.dump
	rm -f lib/*/ebin/*.beam
	rm -rf lib/*/_build
	rm -rf lib/*/.rebar3
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
