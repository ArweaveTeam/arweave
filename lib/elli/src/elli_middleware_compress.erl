%%% @doc Response compression as Elli middleware.
-module(elli_middleware_compress).
-export([postprocess/3]).
-include("elli_util.hrl").

%%
%% Postprocess handler
%%

%%% @doc Postprocess all requests and compress bodies larger than
%%% `compress_byte_size' (`1024' by default).
-spec postprocess(Req, Result, Config) -> Result when
      Req    :: elli:req(),
      Result :: elli_handler:result(),
      Config :: [{compress_byte_size, non_neg_integer()} | tuple()].
postprocess(Req, {ResponseCode, Body}, Config)
  when is_integer(ResponseCode) orelse ResponseCode =:= ok ->
    postprocess(Req, {ResponseCode, [], Body}, Config);

postprocess(Req, {ResponseCode, Headers, Body} = Res, Config)
  when is_integer(ResponseCode) orelse ResponseCode =:= ok ->
    Threshold = proplists:get_value(compress_byte_size, Config, 1024),
    ?IF(not should_compress(Body, Threshold), Res,
        case compress(Body, Req) of
            no_compress ->
                Res;
            {CompressedBody, Encoding} ->
                NewHeaders = [{<<"Content-Encoding">>, Encoding} | Headers],
                {ResponseCode, NewHeaders, CompressedBody}
        end);
postprocess(_, Res, _) ->
    Res.

%%
%% INTERNALS
%%

%% NOTE: Algorithm is either `<<"gzip">>' or `<<"deflate">>'.
-spec compress(Body0 :: elli:body(), Req :: elli:req()) -> Body1 when
      Body1 :: {Compressed :: binary(), Algorithm :: binary()} | no_compress.
compress(Body, Req) ->
    case accepted_encoding(Req) of
        <<"gzip">>    = E -> {zlib:gzip(Body), E};
        <<"deflate">> = E -> {zlib:compress(Body), E};
        _                 -> no_compress
    end.

accepted_encoding(Req) ->
    hd(binary:split(elli_request:get_header(<<"Accept-Encoding">>, Req, <<>>),
                    [<<",">>, <<";">>], [global])).

-spec should_compress(Body, Threshold) -> boolean() when
      Body      :: binary(),
      Threshold :: non_neg_integer().
should_compress(Body, S) ->
    is_binary(Body) andalso byte_size(Body) >= S orelse
        is_list(Body) andalso iolist_size(Body) >= S.
