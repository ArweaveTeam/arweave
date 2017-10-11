-module(elli_request).
-include("elli.hrl").
-include("elli_util.hrl").

-export([send_chunk/2
         , async_send_chunk/2
         , chunk_ref/1
         , close_chunk/1
         , path/1
         , raw_path/1
         , query_str/1
         , get_header/2
         , get_header/3
         , get_arg_decoded/2
         , get_arg_decoded/3
         , get_arg/2
         , get_arg/3
         , get_args/1
         , get_args_decoded/1
         , post_arg/2
         , post_arg/3
         , post_arg_decoded/2
         , post_arg_decoded/3
         , post_args/1
         , post_args_decoded/1
         , body_qs/1
         , headers/1
         , peer/1
         , method/1
         , body/1
         , get_range/1
         , to_proplist/1
         , is_request/1
        ]).

%%
%% Helpers for working with a #req{}
%%


%% @doc: Returns path split into binary parts.
path(#req{path = Path})          -> Path.
raw_path(#req{raw_path = Path})  -> Path.
headers(#req{headers = Headers}) -> Headers.
method(#req{method = Method})    -> Method.
body(#req{body = Body})          -> Body.

peer(#req{socket = Socket} = Req) ->
    case get_header(<<"X-Forwarded-For">>, Req, undefined) of
        undefined ->
            case elli_tcp:peername(Socket) of
                {ok, {Address, _}} ->
                    list_to_binary(inet_parse:ntoa(Address));
                {error, _} ->
                    undefined
            end;
        Ip ->
            Ip
    end.


get_header(Key, #req{headers = Headers}) ->
    proplists:get_value(Key, Headers).

get_header(Key, #req{headers = Headers}, Default) ->
    proplists:get_value(Key, Headers, Default).

get_arg(Key, #req{} = Req) ->
    get_arg(Key, Req, undefined).

get_arg(Key, #req{args = Args}, Default) ->
    proplists:get_value(Key, Args, Default).

get_arg_decoded(Key, #req{} = Req) ->
    get_arg_decoded(Key, Req, undefined).

get_arg_decoded(Key, #req{args = Args}, Default) ->
    case proplists:get_value(Key, Args) of
        undefined -> Default;
        EncodedValue ->
            list_to_binary(http_uri:decode(binary_to_list(EncodedValue)))
    end.

%% @doc Parses application/x-www-form-urlencoded body into a proplist
body_qs(#req{body = <<>>}) -> [];
body_qs(#req{body = Body} = Req) ->
    case get_header(<<"Content-Type">>, Req) of
        <<"application/x-www-form-urlencoded">> ->
            elli_http:split_args(Body);
        <<"application/x-www-form-urlencoded;", _/binary>> -> % ; charset=...
            elli_http:split_args(Body);
        _ ->
            erlang:error(badarg)
    end.

post_arg(Key, #req{} = Req) ->
    post_arg(Key, #req{} = Req, undefined).

post_arg(Key, #req{} = Req, Default) ->
    proplists:get_value(Key, body_qs(Req), Default).

post_arg_decoded(Key, #req{} = Req) ->
    post_arg_decoded(Key, #req{} = Req, undefined).

post_arg_decoded(Key, #req{} = Req, Default) ->
    case proplists:get_value(Key, body_qs(Req)) of
        undefined -> Default;
        EncodedValue ->
            list_to_binary(http_uri:decode(binary_to_list(EncodedValue)))
    end.


-spec get_args(#req{}) -> QueryArgs :: proplists:proplist().
%% @doc Returns a proplist of keys and values of the original query
%%      string.  Both keys and values in the returned proplists will
%%      be binaries or the atom `true' in case no value was supplied
%%      for the query value.
get_args(#req{args = Args}) -> Args.

get_args_decoded(#req{args = Args}) ->
    lists:map(fun ({K, true}) ->
                      {K, true};
                  ({K, V}) ->
                      {K, list_to_binary(http_uri:decode(binary_to_list(V)))}
              end, Args).


post_args(#req{} = Req) ->
    body_qs(Req).

post_args_decoded(#req{} = Req) ->
    lists:map(fun ({K, true}) ->
                      {K, true};
                  ({K, V}) ->
                      {K, list_to_binary(http_uri:decode(binary_to_list(V)))}
              end, body_qs(Req)).

-spec query_str(#req{}) -> QueryStr :: binary().
%% @doc Calculates the query string associated with the given Request
%% as a binary.
query_str(#req{raw_path = Path}) ->
    case binary:split(Path, [<<"?">>]) of
        [_, Qs] -> Qs;
        [_]     -> <<>>
    end.


-spec get_range(#req{}) -> [http_range()] | parse_error.
%% @doc: Parses the Range header from the request.
%% The result is either a byte_range_set() or the atom `parse_error'.
%% Use elli_util:normalize_range/2 to get a validated, normalized range.
get_range(#req{headers = Headers})  ->
    case proplists:get_value(<<"Range">>, Headers) of
        <<"bytes=", RangeSetBin/binary>> ->
            parse_range_set(RangeSetBin);
        _  -> []
    end.


-spec parse_range_set(Bin::binary()) -> [http_range()] | parse_error.

parse_range_set(<<ByteRangeSet/binary>>) ->
    RangeBins = binary:split(ByteRangeSet, <<",">>, [global]),
    Parsed = [parse_range(remove_whitespace(RangeBin))
              || RangeBin <- RangeBins],
    case lists:any(fun(parse_error) -> true; (_) -> false end, Parsed) of
        true  -> parse_error;
        false -> Parsed
    end.

-spec parse_range(Bin::binary()) -> http_range() | parse_error.

parse_range(<<$-, SuffixBin/binary>>) ->
    %% suffix-byte-range
    try {suffix, ?b2i(SuffixBin)}
    catch
        error:badarg -> parse_error
    end;
parse_range(<<ByteRange/binary>>) ->
    case binary:split(ByteRange, <<"-">>) of
        %% byte-range without last-byte-pos
        [FirstBytePosBin, <<>>] ->
            try {offset, ?b2i(FirstBytePosBin)}
            catch
                error:badarg -> parse_error
            end;
        %% full byte-range
        [FirstBytePosBin, LastBytePosBin] ->
            try {bytes, ?b2i(FirstBytePosBin), ?b2i(LastBytePosBin)}
            catch
                error:badarg -> parse_error
            end;
        _ -> parse_error
    end.

remove_whitespace(Bin) ->
    binary:replace(Bin,<<" ">>, <<>>, [global]).

%% @doc: Serializes the request record to a proplist. Useful for
%% logging
to_proplist(#req{} = Req) ->
    lists:zip(record_info(fields, req), tl(tuple_to_list(Req))).



%% @doc: Returns a reference that can be used to send chunks to the
%% client. If the protocol does not support it, returns {error,
%% not_supported}.
chunk_ref(#req{version = {1, 1}} = Req) ->
    Req#req.pid;
chunk_ref(#req{}) ->
    {error, not_supported}.


%% @doc: Explicitly close the chunked connection. Returns {error,
%% closed} if the client already closed the connection.
close_chunk(Ref) ->
    send_chunk(Ref, close).

%% @doc: Sends a chunk asynchronously
async_send_chunk(Ref, Data) ->
    Ref ! {chunk, Data}.

%% @doc: Sends a chunk synchronously, if the refrenced process is dead
%% returns early with {error, closed} instead of timing out.
send_chunk(Ref, Data) ->
    case is_ref_alive(Ref) of
        false -> {error, closed};
        true  -> send_chunk(Ref, Data, 5000)
    end.

send_chunk(Ref, Data, Timeout) ->
    Ref ! {chunk, Data, self()},
    receive
        {Ref, ok} ->
            ok;
        {Ref, {error, Reason}} ->
            {error, Reason}
    after Timeout ->
            {error, timeout}
    end.

is_ref_alive(Ref) ->
    case node(Ref) =:= node() of
        true -> is_process_alive(Ref);
        false -> rpc:call(node(Ref), erlang, is_process_alive, [Ref])
    end.

is_request(#req{}) -> true;
is_request(_)      -> false.
