-module(ar_http_req).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-export([body/2, read_body_chunk/3, body_read_time/1]).

-define(AR_HTTP_REQ_BODY, '_ar_http_req_body').
-define(AR_HTTP_REQ_BODY_READ_TIME, '_ar_http_req_body_read_time').

body(Req, SizeLimit) ->
	case maps:get(?AR_HTTP_REQ_BODY, Req, not_set) of
		not_set ->
			StartTime = erlang:monotonic_time(),
			read_complete_body(Req, SizeLimit, StartTime);
		Body ->
			{ok, Body, Req}
	end.

%% @doc The elapsed time (in native units) to read the request body via `read_complete_body()`
body_read_time(Req) ->
	maps:get(?AR_HTTP_REQ_BODY_READ_TIME, Req, undefined).

read_body_chunk(Req, Size, Timeout) ->
	case cowboy_req:read_body(Req, #{ length => Size, period => Timeout }) of
		{_, Chunk, Req2} when byte_size(Chunk) >= Size ->
			prometheus_counter:inc(http_server_accepted_bytes_total,
					[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req2 })], Size),
			{ok, Chunk, Req2};
		{_, Chunk, Req2} ->
			prometheus_counter:inc(http_server_accepted_bytes_total,
					[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req2 })],
					byte_size(Chunk)),
			exit(timeout)
	end.

read_complete_body(Req, SizeLimit, StartTime) ->
	Parent = self(),
	Ref = make_ref(),
	{Pid, MonRef} = spawn_monitor(
		fun() ->
			do_read_body(Req, Parent, Ref)
		end),
	TRef = erlang:send_after(?DEFAULT_HTTP_MAX_BODY_READ_TIME_MS, self(),
		{body_timeout, Ref}),
	Result = accumulate_body(MonRef, Pid, Ref, Req, [], 0, SizeLimit, StartTime),
	erlang:cancel_timer(TRef),
	flush_messages(Ref),
	demonitor(MonRef, [flush]),
	case Result of
		exit_timeout ->
			exit(timeout);
		_ ->
			Result
	end.

accumulate_body(MonRef, Pid, Ref, Req, Acc, Size, SizeLimit, StartTime) ->
	receive
		{Ref, _OkOrMore, _Data, DataSize} when Size + DataSize > SizeLimit ->
			exit(Pid, kill),
			{error, body_size_too_large};
		{Ref, more, Data, DataSize} ->
			NewSize = Size + DataSize,
			accumulate_body(MonRef, Pid, Ref, Req, [Acc | Data],
				NewSize, SizeLimit, StartTime);
		{Ref, ok, Data, _DataSize} ->
			Body = iolist_to_binary([Acc | Data]),
			BodyReadTime = erlang:monotonic_time() - StartTime,
			{ok, Body, with_body_req_fields(Req, Body, BodyReadTime)};
		{body_timeout, Ref} ->
			exit(Pid, kill),
			exit_timeout;
		{'DOWN', MonRef, process, Pid, timeout} ->
			exit_timeout;
		{'DOWN', MonRef, process, Pid, Reason} ->
			{error, Reason}
	after ?DEFAULT_HTTP_READ_BODY_PERIOD_MS + 2000 ->
		exit(Pid, kill),
		exit_timeout
	end.

do_read_body(Req, Parent, Ref) ->
	{MoreOrOk, Data, ReadReq} = cowboy_req:read_body(Req,
		#{ period => ?DEFAULT_HTTP_READ_BODY_PERIOD_MS,
		   timeout => ?DEFAULT_HTTP_READ_BODY_PERIOD_MS + 1000 }),
	DataSize = byte_size(Data),
	prometheus_counter:inc(
		http_server_accepted_bytes_total,
		[ar_prometheus_cowboy_labels:label_value(route, #{ req => Req })],
		DataSize),
	Parent ! {Ref, MoreOrOk, Data, DataSize},
	case MoreOrOk of
		ok ->
			ok;
		more ->
			do_read_body(ReadReq, Parent, Ref)
	end.

flush_messages(Ref) ->
	receive
		{body_timeout, Ref} ->
			flush_messages(Ref);
		{Ref, _, _, _} ->
			flush_messages(Ref)
	after 0 ->
		ok
	end.

with_body_req_fields(Req, Body, BodyReadTime) ->
	Req#{
		?AR_HTTP_REQ_BODY => Body,
		?AR_HTTP_REQ_BODY_READ_TIME => BodyReadTime }.
