.DEFAULT_GOAL = test_all

test_all: test test_apps

test: all
	@erl -noshell -s ar test -pa ebin/ -s init stop

test_apps: all
	@erl -noshell -s ar test_apps -pa ebin/ -s init stop

all: ebin
	erlc +export_all -o ebin/ src/ar.erl
	erl -noshell -s ar rebuild -pa ebin/ -s init stop

ebin:
	mkdir -p ebin

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
