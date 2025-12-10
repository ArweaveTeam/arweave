%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Diagnostic Module.
%%%
%%% This module has been created to display detailed information about
%%% an Arweave application, including information from the BEAM as
%%% well.
%%%
%%% @todo create diagnostic for epmd
%%% @todo create diagnostic for timers
%%% @end
%%%===================================================================
-module(arweave_diagnostic).
-compose(warnings_as_errors).
-compile({no_auto_import,[processes/0]}).
-compile({no_auto_import,[process_info/1]}).
-export([
	all/0,
	select/1,
	cpu/0,
	memory/0,
	memory_worst/0,
	network/0,
	processes/0,
	sockets/0,
	arweave/0,
	ets/0,
	dets/0,
	rocksdb/0
]).
-include_lib("kernel/include/logger.hrl").

-type diagnostic() :: cpu | memory | memory_worst | processes |
	arweave | ets | dets.

%%--------------------------------------------------------------------
%% @doc returns all diagnostic available.
%% @end
%%--------------------------------------------------------------------
-spec all() -> proplists:proplist().

all() ->
	select([
		cpu,
		memory,
		memory_worst,
		network,
		processes,
		arweave,
		ets,
		dets,
		rocksdb
	]).

%%--------------------------------------------------------------------
%% @doc returns a subset of supported diagnostics.
%% @end
%%--------------------------------------------------------------------
-spec select([diagnostic()]) -> proplists:proplist().

select(List) -> select(List, []).

select([], Buffer) -> Buffer;
select([cpu|Rest], Buffer) ->
	select(Rest,[{cpu,cpu()}|Buffer]);
select([memory|Rest], Buffer) ->
	select(Rest,[{memory,memory()}|Buffer]);
select([memory_worst|Rest], Buffer) ->
	select(Rest,[{memory_worst,memory_worst()}|Buffer]);
select([network|Rest], Buffer) ->
	select(Rest,[{network,network()}|Buffer]);
select([processes|Rest], Buffer) ->
	select(Rest,[{processes,processes()}|Buffer]);
select([arweave|Rest], Buffer) ->
	select(Rest,[{arweave,arweave()}|Buffer]);
select([ets|Rest], Buffer) ->
	select(Rest,[{ets_tables,ets()}|Buffer]);
select([dets|Rest], Buffer) ->
	select(Rest,[{dets_tables,dets()}|Buffer]);
select([rocksdb|Rest], Buffer) ->
	select(Rest,[{rocksdb,rocksdb()}|Buffer]);
select([_|Rest], Buffer) ->
	select(Rest, Buffer).

%%--------------------------------------------------------------------
%% @doc Returns cpu diagnostic. Most of the cpu information are being
%% collected using `cpu_sup' module, this is then, a dependency.
%% @end
%%--------------------------------------------------------------------
-spec cpu() -> proplists:proplist().

cpu() ->
	try
		display([
			{nprocs, cpu_sup:nprocs()},
			{avg1, cpu_sup:avg1()},
			{avg5, cpu_sup:avg5()},
			{avg15, cpu_sup:avg15()},
			{util, cpu_sup:util()},
			{ping, cpu_sup:ping()}
		], cpu)
	catch _:_  ->
		 ?LOG_WARNING("cpu_sup not started"),
		 []
	end.

%%--------------------------------------------------------------------
%% @doc Returns memory diagnostic. Most of the memory information are
%% collected using `memsup' module. This is then a dependency.
%% @end
%%--------------------------------------------------------------------
-spec memory() -> proplists:proplist().

memory() ->
	try
		Memory = memsup:get_system_memory_data(),
		display(Memory, memory)
	catch _:_ ->
		 ?LOG_WARNING("memsup not started"),
		 []
	end.

%%--------------------------------------------------------------------
%% @doc Returns the pid using the greatest amount of memory. This
%% function is using `memsup' module.
%% @end
%%--------------------------------------------------------------------
-spec memory_worst() -> proplists:proplist().

memory_worst() ->
	try
		memsup:get_memory_data()
	of
		{Total, Allocated, {Pid, PidAllocated}} ->
			Info = process_info(Pid),
			Name = proplists:get_value(registered_name, Info),
			HeapSize = proplists:get_value(heap_size, Info),
			TotalHeapSize = proplists:get_value(total_heap_size, Info),
			StackSize = proplists:get_value(stack_size, Info),
			Reductions = proplists:get_value(reductions, Info),
			MsgQueue = proplists:get_value(message_queue_len, Info),
			Status = proplists:get_value(status, Info),
			display([
				{total, Total},
				{allocated, Allocated},
				{worst_pid, Pid},
				{worst_pid_allocated, PidAllocated},
				{worst_pid_name, Name},
				{worst_pid_total_heap_size, TotalHeapSize},
				{worst_pid_heap_size, HeapSize},
				{worst_pid_stack_size, StackSize},
				{worst_pid_reductions, Reductions},
				{worst_pid_message_queue, MsgQueue},
				{worst_pid_status, Status}
			], memory_worst)
	catch _:_ ->
		?LOG_WARNING("memsup is not started"),
		[]
	end.

%%--------------------------------------------------------------------
%% @doc Returns network diagnostic.
%% @end
%%--------------------------------------------------------------------
-spec network() -> proplists:proplist().

network() ->
	Sockets = sockets(),
	display([
		{sockets, length(Sockets)}
	], network).

%%--------------------------------------------------------------------
%% @doc Returns sockets (network ports) diagnostic.
%% @end
%%--------------------------------------------------------------------
sockets() ->
	Ports = erlang:ports(),
	Sockets = lists:filter(fun is_network_port/1, Ports),
	[
		{socket, socket_info(S)}
		|| S <- Sockets
	].

%%--------------------------------------------------------------------
%% @doc Returns processes diagnostic.
%% @end
%%--------------------------------------------------------------------
-spec processes() -> proplists:proplist().

processes() ->
	Processes = erlang:processes(),
	[
		process_info(P)
		|| P <- Processes
	].

%%--------------------------------------------------------------------
%% @hidden
%% @doc wrapper around `erlang:process_info/1'.
%% @end
%%--------------------------------------------------------------------
-spec process_info(Pid) -> Return when
	Pid :: pid(),
	Return :: proplists:proplist().

process_info(Pid) ->
	try erlang:process_info(Pid) of
		undefined ->
			[];
		Info ->
			process_info2(Pid, Info)
	catch _:_ ->
		[]
	end.

process_info2(Pid, Info) ->
	RegisteredName = proplists:get_value(registered_name, Info),
	Status = proplists:get_value(status, Info),
	MsgQueueLen = proplists:get_value(message_queue_len, Info),
	TrapExit = proplists:get_value(trap_exit, Info),
	Priority = proplists:get_value(priority, Info),
	GroupLeader = proplists:get_value(group_leader, Info),
	TotalHeapSize = proplists:get_value(total_heap_size, Info),
	HeapSize = proplists:get_value(heap_size, Info),
	StackSize = proplists:get_value(stack_size, Info),
	Reductions = proplists:get_value(reductions, Info),
	GC = proplists:get_value(garbage_collection, Info),
	GC_MinBinVHeapSize = proplists:get_value(min_bin_vheap_size, GC),
	GC_MinHeapSize = proplists:get_value(min_heap_size, GC),
	GC_FullsweepAfter = proplists:get_value(fullsweep_after, GC),
	GC_MinorGCS = proplists:get_value(minor_gcs, GC),
	CurrentFunction =
		try
			erlang:process_info(Pid, current_location)
		of
			{current_location,{M,F,A,_}} ->
				io_lib:format("~s:~s/~b", [M,F,A]);
			_ ->
				undefined
		catch _:_ ->
			      undefined
		end,
	Memory =
		try
			erlang:process_info(Pid, memory)
		of
			{memory, Mem} ->
				Mem;
			_ ->
				undefined
		catch _:_ ->
			      undefined
		end,
	display([
		{pid, Pid},
		{status, Status},
	 	{location, CurrentFunction},
	 	{memory, Memory},
		{group_leader, GroupLeader},
		{heap_size, HeapSize},
		{message_queue_len, MsgQueueLen},
		{priority, Priority},
		{reductions, Reductions},
		{registered_name, RegisteredName},
		{stack_size, StackSize},
		{total_heap_size, TotalHeapSize},
		{trap_exit, TrapExit},
		{gc_min_bin_vheap_size, GC_MinBinVHeapSize},
		{gc_min_heap_size, GC_MinHeapSize},
		{gc_fullsweep_after, GC_FullsweepAfter},
		{gc_minor_gcs, GC_MinorGCS}
	], process).

%%--------------------------------------------------------------------
%% @hidden
%% @doc Check if a port is a network socket (tcp, udp, sctp).
%% @end
%%--------------------------------------------------------------------
-spec is_network_port(Port) -> Return when
	Port :: port(),
	Return :: boolean().

is_network_port(Port) ->
	try
		Info = erlang:port_info(Port),
		proplists:get_value(name, Info)
	of
		"tcp_inet" -> true;
		"udp_inet" -> true;
		"sctp_inet" -> true;
		_ -> false
	catch _:_ ->
		false
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc A function helper to gather information about a socket.
%% @end
%%--------------------------------------------------------------------
-spec socket_info(Socket) -> Return when
	Socket :: port(),
	Return :: proplists:proplist().

socket_info(Socket) ->
	try inet:info(Socket) of
		undefined ->
			[];
		Info ->
			socket_info2(Socket, Info)
	catch _:_ ->
		[]
	end.

socket_info2(Socket, Info) ->
	Counters = maps:get(counters, Info, #{}),
	Active = maps:get(active, Info, undefined),
	Domain = maps:get(domain, Info, undefined),
	NumAcceptors = maps:get(num_acceptors, Info, undefined),
	NumReaders = maps:get(num_readers, Info, undefined),
	NumWriters = maps:get(num_writers, Info, undefined),
	Protocol = maps:get(protocol, Info, undefined),
	RecvAvg = maps:get(recv_avg, Counters, undefined),
	RecvCnt = maps:get(recv_cnt, Counters, undefined),
	RecvDvi = maps:get(recv_dvi, Counters, undefined),
	RecvMax = maps:get(recv_max, Counters, undefined),
	RecvOct = maps:get(recv_oct, Counters, undefined),
	SendAvg = maps:get(send_avg, Counters, undefined),
	SendCnt = maps:get(send_cnt, Counters, undefined),
	SendMax = maps:get(send_max, Counters, undefined),
	SendOct = maps:get(send_oct, Counters, undefined),
	SendPend = maps:get(send_pend, Counters, undefined),

	{PeerIP,PeerPort} =
		case inet:peername(Socket) of
			{ok, {PI,PP}} ->
				{inet:ntoa(PI), PP};
			_ ->
				{undefined, undefined}
		end,
	{SockIP,SockPort} =
		case inet:sockname(Socket) of
			{ok, {SI, SP}} ->
				{inet:ntoa(SI), SP};
			_ ->
				{undefined, undefined}
		end,
	display([
		{socket, Socket},
		{active, Active},
		{domain, Domain},
		{protocol, Protocol},
		{peer_ip, PeerIP},
		{peer_port, PeerPort},
		{sock_ip, SockIP},
		{sock_port, SockPort},
		{num_acceptors, NumAcceptors},
		{num_readers, NumReaders},
		{num_writers, NumWriters},
		{recv_oct, RecvOct},
		{recv_avg, RecvAvg},
		{recv_cnt, RecvCnt},
		{recv_dvi, RecvDvi},
		{recv_max, RecvMax},
		{send_avg, SendAvg},
		{send_cnt, SendCnt},
		{send_max, SendMax},
		{send_oct, SendOct},
		{send_pend, SendPend}
	], socket).

%%--------------------------------------------------------------------
%% @doc Returns arweave diagnostic. This function should display more
%% information than the `processes/0' diagnostic. The application to
%% be checked are `arweave', `arweave_config'.
%%
%%   - check processes (like in `processes/0')
%%   - check ETS tables
%%   - check workers status
%%
%% This function will become huge, and should probably be migrated in
%% its own module called.
%%
%% @end
%%--------------------------------------------------------------------
-spec arweave() -> proplists:proplist().

arweave() ->
	arweave_processes().

arweave_processes() ->
	case get_process_group_leader(ar_sup) of
		undefined ->
			[];
		Leader ->
			arweave_processes(Leader)
	end.

arweave_processes(Leader) ->
	Processes = erlang:processes(),
	[
	 	display(N, arweave_processes)
	 	|| N <- [
			process_info(P)
			|| P <- Processes
		],
		proplists:get_value(group_leader, N) =:= Leader
	].

%%--------------------------------------------------------------------
%% @hidden
%% @doc extract process group leader of a pid or registered process.
%% @end
%%--------------------------------------------------------------------
get_process_group_leader(undefined) -> undefined;
get_process_group_leader(Atom) when is_atom(Atom) ->
	get_process_group_leader(whereis(Atom));
get_process_group_leader(Pid) when is_pid(Pid) ->
	try
		erlang:process_info(Pid, group_leader)
	of
		{group_leader, GL} ->
			GL;
		_ ->
			undefined
	catch _:_ ->
		      undefined
	end.

%%--------------------------------------------------------------------
%% @doc Returns ets diagnostic.
%% @end
%%--------------------------------------------------------------------
-spec ets() -> proplists:proplist().

ets() ->
	Ets = ets:all(),
	ets(Ets, []).

ets([], Buffer) -> Buffer;
ets([Ets|Rest], Buffer) ->
	try
		Info = display(
			ets:info(Ets),
			ets
		),
		NewBuffer = [{Ets, Info}|Buffer],
		ets(Rest, NewBuffer)
	catch _:_ ->
		ets(Rest, Buffer)
	end.

%%--------------------------------------------------------------------
%% @doc Returns dets diagnostic.
%% @end
%%--------------------------------------------------------------------
-spec dets() -> proplists:proplist().

dets() ->
	Dets = dets:all(),
	dets(Dets, []).

dets([], Buffer) -> Buffer;
dets([Dets|Rest], Buffer) ->
	try
		Info = display(
			dets:info(Dets),
			dets
		),
		NewBuffer = [{Dets, Info}|Buffer],
		dets(Rest, NewBuffer)
	catch _:_ ->
		dets(Rest, Buffer)
	end.

%% record extracted from ar_kv module. This record is used to store
%% rocksdb information used by arweave.
-record(db, {
	name :: term() | undefined,
	filepath :: file:filename_all(),
	db_options :: rocksdb:db_options(),
	db_handle :: rocksdb:db_handle() | undefined,
	cf_names = undefined :: [term()],
	cf_descriptors = undefined :: [rocksdb:cf_descriptor()],
	cf_handle = undefined :: rocksdb:cf_handle()
}).
%%--------------------------------------------------------------------
%% @doc Returns rocksdb diagnostic. This function get the list of
%% opened database by checking the content of `ar_kv' ETS table.
%%--------------------------------------------------------------------
-spec rocksdb() -> proplists:proplist().
rocksdb() ->
	case ets:info(ar_kv) of
		undefined -> [];
		Info -> rocksdb(Info)
	end.

rocksdb(Info) ->
	Ets = proplists:get_value(id, Info),
	[
		{rocksdb, rocksdb_struct(Db)}
		|| Db <- ets:tab2list(Ets)
	].

rocksdb_struct(#db{filepath = Filepath, db_handle = Handle}) ->
	Properties = rocksdb_properties(),
	Result = rocksdb_properties(Handle, Properties, []),
	display([
			{filepath, Filepath},
			{handle,Handle}
			|Result
		],
		rocksdb
	).

%%--------------------------------------------------------------------
%% @hidden
%% @doc These properties should always returns an integer formatted
%% as binary.
%% @end
%%--------------------------------------------------------------------
-spec rocksdb_properties() -> [binary()].

rocksdb_properties() ->
	[
		<<"rocksdb.actual-delayed-write-rate">>,
		<<"rocksdb.background-errors">>,
		<<"rocksdb.base-level">>,
		<<"rocksdb.block-cache-capacity">>,
		<<"rocksdb.block-cache-pinned-usage">>,
		<<"rocksdb.block-cache-usage">>,
		<<"rocksdb.compaction-pending">>,
		<<"rocksdb.current-super-version-number">>,
		<<"rocksdb.cur-size-active-mem-table">>,
		<<"rocksdb.cur-size-all-mem-tables">>,
		<<"rocksdb.estimate-live-data-size">>,
		<<"rocksdb.estimate-num-keys">>,
		<<"rocksdb.estimate-pending-compaction-bytes">>,
		<<"rocksdb.estimate-table-readers-mem">>,
		<<"rocksdb.is-file-deletions-enabled">>,
		<<"rocksdb.is-write-stopped">>,
		<<"rocksdb.live-blob-file-size">>,
		<<"rocksdb.live-sst-files-size">>,
		<<"rocksdb.mem-table-flush-pending">>,
		<<"rocksdb.min-log-number-to-keep">>,
		<<"rocksdb.min-obsolete-sst-number-to-keep">>,
		<<"rocksdb.num-blob-files">>,
		<<"rocksdb.num-deletes-active-mem-table">>,
		<<"rocksdb.num-deletes-active-mem-table">>,
		<<"rocksdb.num-deletes-imm-mem-tables">>,
		<<"rocksdb.num-entries-active-mem-table">>,
		<<"rocksdb.num-entries-imm-mem-tables">>,
		<<"rocksdb.num-files-at-level0">>,
		<<"rocksdb.num-files-at-level1">>,
		<<"rocksdb.num-files-at-level2">>,
		<<"rocksdb.num-files-at-level3">>,
		<<"rocksdb.num-files-at-level4">>,
		<<"rocksdb.num-immutable-mem-table">>,
		<<"rocksdb.num-immutable-mem-table-flushed">>,
		<<"rocksdb.num-live-versions">>,
		<<"rocksdb.num-running-compactions">>,
		<<"rocksdb.num-running-flushes">>,
		<<"rocksdb.num-snapshots">>,
		<<"rocksdb.size-all-mem-tables">>,
		<<"rocksdb.total-blob-file-size">>,
		<<"rocksdb.total-sst-files-size">>
	].

%%--------------------------------------------------------------------
%% @hidden
%% @doc loop over the properties and convert them into a proplist.
%% @end
%%--------------------------------------------------------------------
-spec rocksdb_properties(Handle, Properties, Buffer) -> Return when
	Handle :: reference(),
	Properties :: [binary()],
	Buffer :: proplists:proplist(),
	Return :: proplists:proplist().

rocksdb_properties(_, [], Buffer) -> Buffer;
rocksdb_properties(Handle, [Property|Rest], Buffer)
	when is_binary(Property) ->
		try
			{ok, Raw} = rocksdb:get_property(Handle, Property),
			Value = binary_to_integer(Raw),
			% converting the value for presentation
			% purpose.
			String = binary_to_list(Property),
			NewBuffer = [{String,Value}|Buffer],
			rocksdb_properties(Handle,Rest,NewBuffer)
		catch
			_:_ ->
				rocksdb_properties(Handle,Rest,Buffer)
		end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc display diagnostics via logs.
%% @end
%%--------------------------------------------------------------------
display(Diagnostic, Category) ->
	Message = [{diagnostic, Category}|Diagnostic],
	?LOG_INFO(Message),
	Diagnostic.
