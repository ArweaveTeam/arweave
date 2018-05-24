all: deps compile check test

deps:
	rebar get-deps

compile:
	rebar compile

run: compile
	sh start.sh

clean:
	rebar clean
	rm -fr ebin .ct test/*.beam

check:
	rebar eunit skip_deps=true

test: deps compile check
	##rebar ct
	#mkdir -p .ct
	#ct_run -dir test -logdir .ct -pa ebin

dist: deps compile
	echo TODO

.PHONY: all deps compile check test run clean dist
.SILENT:

