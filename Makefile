.DEFAULT_GOAL = test_all

test_all: test test_apps

test: all
	@erl -noshell -s ar test -pa ebin/ -s init stop

test_apps: all
	@erl -noshell -s ar test_apps -pa ebin/ -s init stop

test_networks: all
	@erl -s ar test_networks -pa ebin/

realistic: all
	@erl -noshell -s ar start -s ar_test_sup start realistic -pa ebin/

log:
	tail -f logs/`ls -t logs |  head -n 1`

all: ebin logs
	erlc +export_all -o ebin/ src/ar.erl
	erl -noshell -s ar rebuild -pa ebin/ -s init stop

ebin:
	mkdir -p ebin

logs:
	mkdir -p logs

session: all
	erl -s ar start -pa ebin/

sim_realistic: all
	erl -pa ebin/ -s ar_network spawn_and_mine realistic

sim_hard: all
	erl -pa ebin/ -s ar_network spawn_and_mine hard

clean:
	rm -rf ebin/*
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
