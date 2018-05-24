.DEFAULT_GOAL = test_all

test_all: test test_apps

test: all
	@erl -noshell -s ar test_coverage -pa ebin/ -s init stop

test_apps: all
	@erl -noshell -s ar test_apps -pa ebin/ -s init stop

test_networks: all
	@erl -s ar start -s ar test_networks -pa ebin/

tnt: test

no-vlns: test_networks

realistic: all
	@erl -noshell -s ar start -s ar_test_sup start realistic -pa ebin/

log:
	tail -f logs/`ls -t logs |  head -n 1`

catlog:
	cat logs/`ls -t logs | head -n 1`

all: ebin logs blocks
	rm -rf priv
	cd lib/jiffy && ./rebar compile && cd ../.. && mv lib/jiffy/priv ./
	erlc +export_all -o ebin/ src/ar.erl
	erl -noshell -s ar rebuild -pa ebin/ -s init stop

ebin:
	mkdir -p ebin

logs:
	mkdir -p logs

blocks:
	mkdir -p blocks

docs: all
	mkdir -p docs
	(cd docs && erl -noshell -s ar docs -pa ../ebin -s init stop)

session: all
	erl -s ar start -pa ebin/

sim_realistic: all
	erl -pa ebin/ -s ar_network spawn_and_mine realistic

sim_hard: all
	erl -pa ebin/ -s ar_network spawn_and_mine hard

clean:
	rm -rf ebin docs blocks logs txs
	rm -f erl_crash.dump

status: clean
	hg status

history:
	hg history | tac

commit:
	EDITOR=vim hg commit
	hg push

update: clean
	hg pull
	hg update

diff:
	hg extdiff -p meld

todo:
	grep --color --line-number --recursive TODO "src"
