-module(ar_sqlite3).
-export([start_link/0, stop/0]).
-export([libversion/0, libversion/1]).
-export([open/1, open/2, close/1, close/2]).
-export([prepare/2, bind/2, step/1, reset/1, finalize/1]).
-export([prepare/3, bind/3, step/2, reset/2, finalize/2]).
-export([exec/2, exec/3]).
-export([init/0]).

-define(DEFAULT_TIMEOUT, 5000).

%% Public API

start_link() ->
	Pid = spawn_link(?MODULE, init, []),
	register(?MODULE, Pid),
	ok.

stop() ->
	?MODULE ! stop,
	ok.

libversion() -> libversion(?DEFAULT_TIMEOUT).
libversion(Timeout) ->
	call_port(libversion, Timeout).

open(Filename) -> open(Filename, ?DEFAULT_TIMEOUT).
open(Filename, Timeout) ->
	call_port({open, Filename}, Timeout).

close(Addr) -> close(Addr, ?DEFAULT_TIMEOUT).
close(Addr, Timeout) ->
	call_port({close, Addr}, Timeout).

prepare(DB, SQL) -> prepare(DB, SQL, ?DEFAULT_TIMEOUT).
prepare(DB, SQL, Timeout) ->
	call_port({prepare, DB, SQL}, Timeout).

bind(Stmt, Params) -> bind(Stmt, Params, ?DEFAULT_TIMEOUT).
bind(Stmt, Params, Timeout) ->
	call_port({bind, Stmt, Params}, Timeout).

step(Stmt) -> step(Stmt, ?DEFAULT_TIMEOUT).
step(Stmt, Timeout) ->
	call_port({step, Stmt}, Timeout).

reset(Stmt) -> reset(Stmt, ?DEFAULT_TIMEOUT).
reset(Stmt, Timeout) ->
	call_port({reset, Stmt}, Timeout).

finalize(Stmt) -> finalize(Stmt, ?DEFAULT_TIMEOUT).
finalize(Stmt, Timeout) ->
	call_port({finalize, Stmt}, Timeout).

exec(DB, SQL) -> exec(DB, SQL, ?DEFAULT_TIMEOUT).
exec(DB, SQL, Timeout) ->
	call_port({exec, DB, SQL}, Timeout).

%% Process callbacks

init() ->
	PrivDir = code:priv_dir(ar_sqlite3),
	Executable = filename:join([PrivDir, "ar_sqlite3_driver"]),
	Port = open_port({spawn_executable, Executable}, [{packet, 2}, binary]),
	monitor(port, Port),
	loop(Port).

%% Private functions

call_port(Msg, Timeout) ->
	Ref = make_ref(),
	?MODULE ! {call, self(), Ref, Msg},
	receive
		{?MODULE, Ref, Reply} -> Reply
	after Timeout ->
		error(timeout)
	end.

loop(Port) ->
	receive
		{call, From, Ref, Msg} ->
			Port ! {self(), {command, term_to_binary(Msg)}},
			receive
				{Port, {data, Data}} ->
					From ! {?MODULE, Ref, binary_to_term(Data)};
				{'DOWN', _MonitorRef, port, Port, _Reason} ->
					exit(port_terminated)
			end,
			loop(Port);
		stop ->
			Port ! {self(), close},
			receive
				{Port, closed} ->
					ok
			end;
		{'DOWN', _MonitorRef, port, Port, _Reason} ->
			exit(port_terminated)
		end.
