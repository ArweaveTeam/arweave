%%%=============================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd
%%% @author Diana Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc
%%%
%%% @end
%%%=============================================================================
-module(fusco_protocol).
-copyright("2013, Erlang Solutions Ltd.").

-include("fusco.hrl").

-define(SIZE(Data, Response),
        Response#response{size = Response#response.size + byte_size(Data)}).
-define(RECEPTION(Data, Response), Response#response{size = byte_size(Data),
                             in_timestamp = os:timestamp()}).
-define(TOUT, 1000).
%% Latency is here defined as the time from the start of packet
%% transmission to the start of packet reception

%% API
-export([recv/2, recv/3,
     decode_cookie/1]).

%% TEST
-export([decode_header_value/5, decode_header_value/6,
     decode_header/3, decode_header/4]).

%% TODO handle partial downloads

recv(Socket, Ssl) ->
    recv(Socket, Ssl, infinity).

recv(Socket, Ssl, Timeout) ->
    case fusco_sock:recv(Socket, Ssl, Timeout) of
    {ok, Data} ->
        decode_status_line(<< Data/binary >>,
                           ?RECEPTION(Data,
                                      #response{socket = Socket, ssl = Ssl}),
                           Timeout);
    {error, Reason} ->
        {error, Reason}
    end.

decode_status_line(<<"HTTP/1.0\s",C1,C2,C3,$\s,Rest/bits>>, Response,
                   Timeout) ->
    decode_reason_phrase(Rest, <<>>, Response#response{version = {1,0},
                               status_code = <<C1,C2,C3>>}, Timeout);
decode_status_line(<<"HTTP/1.1\s",C1,C2,C3,$\s,Rest/bits>>, Response,
                   Timeout) ->
    decode_reason_phrase(Rest, <<>>, Response#response{version = {1,1},
                               status_code = <<C1,C2,C3>>}, Timeout);
decode_status_line(Bin, Response = #response{size = Size},
                   Timeout) when Size < 13 ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_status_line(<<Bin/binary, Data/binary>>, ?SIZE(Data, Response),
                           Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_status_line(_, _, _) ->
    {error, status_line}.

decode_reason_phrase(<<>>, Acc, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_reason_phrase(Data, Acc, ?SIZE(Data, Response), Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_reason_phrase(<<$\r>>, Acc, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_reason_phrase(<<$\r, Data/binary>>, Acc, ?SIZE(Data, Response),
                             Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_reason_phrase(<<$\n, Rest/bits>>, Acc, Response, Timeout) ->
    decode_header(Rest, <<>>, Response#response{reason = Acc}, Timeout);
decode_reason_phrase(<<$\r,$\n, Rest/bits>>, Acc, Response, Timeout) ->
    decode_header(Rest, <<>>, Response#response{reason = Acc}, Timeout);
decode_reason_phrase(<<C, Rest/bits>>, Acc, Response, Timeout) ->
    decode_reason_phrase(Rest, <<Acc/binary, C>>, Response, Timeout).

decode_header(Data, Acc, Response) ->
    decode_header(Data, Acc, Response, infinity).

decode_header(<<>>, Acc, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header(Data, Acc, ?SIZE(Data, Response), Timeout);
    {error, closed} ->
        case Acc of
        <<>> ->
            decode_body(<<>>, Response, Timeout);
        _ ->
            {error, closed}
        end;
    {error, Reason} ->
        {error, Reason}
    end;
decode_header(<<$\r>>, Acc, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header(<<$\r, Data/binary>>, Acc, ?SIZE(Data, Response),
                      Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_header(<<$\s, Rest/bits>>, Acc, Response, Timeout) ->
    decode_header(Rest, Acc, Response, Timeout);
decode_header(<<$:, Rest/bits>>, Header, Response, Timeout) ->
    decode_header_value_ws(Rest, Header, Response, Timeout);
decode_header(<<$\n, Rest/bits>>, <<>>, Response, Timeout) ->
    decode_body(Rest, Response, Timeout);
decode_header(<<$\r, $\n, Rest/bits>>, <<>>, Response, Timeout) ->
    decode_body(Rest, Response, Timeout);
decode_header(<<$\r, $\n, _Rest/bits>>, _, _Response, _Timeout) ->
    {error, header};
decode_header(<<$A, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $a>>, Response, Timeout);
decode_header(<<$B, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $b>>, Response, Timeout);
decode_header(<<$C, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $c>>, Response, Timeout);
decode_header(<<$D, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $d>>, Response, Timeout);
decode_header(<<$E, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $e>>, Response, Timeout);
decode_header(<<$F, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $f>>, Response, Timeout);
decode_header(<<$G, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $g>>, Response, Timeout);
decode_header(<<$H, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $h>>, Response, Timeout);
decode_header(<<$I, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $i>>, Response, Timeout);
decode_header(<<$J, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $j>>, Response, Timeout);
decode_header(<<$K, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $k>>, Response, Timeout);
decode_header(<<$L, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $l>>, Response, Timeout);
decode_header(<<$M, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $m>>, Response, Timeout);
decode_header(<<$N, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $n>>, Response, Timeout);
decode_header(<<$O, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $o>>, Response, Timeout);
decode_header(<<$P, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $p>>, Response, Timeout);
decode_header(<<$Q, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $q>>, Response, Timeout);
decode_header(<<$R, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $r>>, Response, Timeout);
decode_header(<<$S, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $s>>, Response, Timeout);
decode_header(<<$T, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $t>>, Response, Timeout);
decode_header(<<$U, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $u>>, Response, Timeout);
decode_header(<<$V, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $v>>, Response, Timeout);
decode_header(<<$W, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $w>>, Response, Timeout);
decode_header(<<$X, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $x>>, Response, Timeout);
decode_header(<<$Y, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $y>>, Response, Timeout);
decode_header(<<$Z, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, $z>>, Response, Timeout);
decode_header(<<C, Rest/bits>>, Header, Response, Timeout) ->
    decode_header(Rest, <<Header/binary, C>>, Response, Timeout).

decode_header_value_ws(<<$\s, Rest/bits>>, H, S, Timeout) ->
    decode_header_value_ws(Rest, H, S, Timeout);
decode_header_value_ws(<<$\t, Rest/bits>>, H, S, Timeout) ->
    decode_header_value_ws(Rest, H, S, Timeout);
decode_header_value_ws(Rest, <<"connection">> = H, S, Timeout) ->
    decode_header_value_lc(Rest, H, <<>>, <<>>, S, Timeout);
decode_header_value_ws(Rest, <<"transfer-encoding">> = H, S, Timeout) ->
    decode_header_value_lc(Rest, H, <<>>, <<>>, S, Timeout);
decode_header_value_ws(Rest, H, S, Timeout) ->
    decode_header_value(Rest, H, <<>>, <<>>, S, Timeout).

decode_header_value(Data, H, V, T, Response) ->
    decode_header_value(Data, H, V, T, Response, infinity).

decode_header_value(<<>>, H, V, T, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header_value(Data, H, V, T, ?SIZE(Data, Response), Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_header_value(<<$\r>>, H, V, T, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header_value(<<$\r, Data/binary>>, H, V, T,
                            ?SIZE(Data, Response), Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_header_value(<<$\n, Rest/bits>>, <<"content-length">> = H, V, _T,
                    Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                        content_length = binary_to_integer(V)}, Timeout);
decode_header_value(<<$\n, Rest/bits>>, <<"set-cookie">> = H, V, _T, Response,
                    Timeout) ->
    decode_header(Rest, <<>>, Response#response{cookies = [decode_cookie(V)
                               | Response#response.cookies],
                      headers = [{H, V} | Response#response.headers]}, Timeout);
decode_header_value(<<$\n, Rest/bits>>, H, V, _T, Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers]},
                  Timeout);
decode_header_value(<<$\r, $\n, Rest/bits>>, <<"set-cookie">> = H, V, _T,
                    Response, Timeout) ->
    decode_header(Rest, <<>>, Response#response{cookies = [decode_cookie(V)
                             | Response#response.cookies],
                      headers = [{H, V} | Response#response.headers]}, Timeout);
decode_header_value(<<$\r,$\n, Rest/bits>>, <<"content-length">> = H, V, _T,
                    Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                                    content_length = binary_to_integer(V)},
                  Timeout);
decode_header_value(<<$\r, $\n, Rest/bits>>, H, V, _T, Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers]},
                  Timeout);
decode_header_value(<<$\s, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value(Rest, H, V, <<T/binary, $\s>>, Response, Timeout);
decode_header_value(<<$\t, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value(Rest, H, V, <<T/binary, $\t>>, Response, Timeout);
decode_header_value(<<C, Rest/bits>>, H, V, <<>>, Response, Timeout) ->
    decode_header_value(Rest, H, <<V/binary, C>>, <<>>, Response, Timeout);
decode_header_value(<<C, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value(Rest, H, <<V/binary, T/binary, C>>, <<>>, Response,
                        Timeout).

decode_header_value_lc(<<>>, H, V, T, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header_value_lc(Data, H, V, T, ?SIZE(Data, Response), Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_header_value_lc(<<$\r>>, H, V, T, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_header_value_lc(<<$\r, Data/binary>>, H, V, T,
                               ?SIZE(Data, Response), Timeout);
    {error, Reason} ->
        {error, Reason}
    end;
decode_header_value_lc(<<$\n, Rest/bits>>, <<"transfer-encoding">> = H, V, _T,
                       Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                                    transfer_encoding = V},
                  Timeout);
decode_header_value_lc(<<$\n, Rest/bits>>, H, V, _T, Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                                    connection = V},
                  Timeout);
decode_header_value_lc(<<$\r, $\n, Rest/bits>>, <<"transfer-encoding">> = H, V,
                       _T, Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                                    transfer_encoding = V},
                  Timeout);
decode_header_value_lc(<<$\r, $\n, Rest/bits>>, H, V, _T, Response, Timeout) ->
    decode_header(Rest, <<>>,
                  Response#response{headers = [{H, V} |
                                               Response#response.headers],
                                    connection = V},
                  Timeout);
decode_header_value_lc(<<$\s, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, V, <<T/binary, $\s>>, Response, Timeout);
decode_header_value_lc(<<$\t, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, V, <<T/binary, $\t>>, Response, Timeout);
decode_header_value_lc(<<$A, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $a>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$B, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $b>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$C, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $c>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$D, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $d>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$E, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $e>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$F, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $f>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$G, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $g>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$H, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $h>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$I, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $i>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$J, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $j>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$K, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $k>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$L, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $l>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$M, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $m>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$N, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $n>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$O, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $o>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$P, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $p>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$Q, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $q>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$R, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $r>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$S, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $s>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$T, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $t>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$U, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $u>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$V, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $v>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$W, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $w>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$X, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $x>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$Y, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $y>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<$Z, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, $z>>, <<>>, Response,
                           Timeout);
decode_header_value_lc(<<C, Rest/bits>>, H, V, T, Response, Timeout) ->
    decode_header_value_lc(Rest, H, <<V/binary, T/binary, C>>, <<>>, Response,
                           Timeout).

%% RFC 6265
%% TODO decode cookie values, this only accepts 'a=b'
decode_cookie(Cookie) ->
    decode_cookie_name(Cookie, <<>>).

decode_cookie_name(<<$\s, Rest/bits>>, N) ->
    decode_cookie_name(Rest, N);
decode_cookie_name(<<$\t, Rest/bits>>, N) ->
    decode_cookie_name(Rest, N);
decode_cookie_name(<<$=, Rest/bits>>, N) ->
    decode_cookie_value(Rest, N, <<>>);
decode_cookie_name(<<C, Rest/bits>>, N) ->
    decode_cookie_name(Rest, <<N/binary, C>>).

decode_cookie_value(<<$\s, Rest/bits>>, N, V) ->
    decode_cookie_value(Rest, N, V);
decode_cookie_value(<<$\t, Rest/bits>>, N, V) ->
    decode_cookie_value(Rest, N, V);
decode_cookie_value(<<$;, Rest/bits>>, N, V) ->
    decode_cookie_av_ws(Rest, #fusco_cookie{name = N, value = V});
decode_cookie_value(<<C, Rest/bits>>, N, V) ->
    decode_cookie_value(Rest, N, <<V/binary, C>>);
decode_cookie_value(<<>>, N, V) ->
    #fusco_cookie{name = N, value = V}.

decode_cookie_av_ws(<<$\s, Rest/bits>>, C) ->
    decode_cookie_av_ws(Rest, C);
decode_cookie_av_ws(<<$\t, Rest/bits>>, C) ->
    decode_cookie_av_ws(Rest, C);
%% We are only interested on Expires, Max-Age, Path, Domain
decode_cookie_av_ws(<<$e, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$e>>);
decode_cookie_av_ws(<<$E, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$e>>);
decode_cookie_av_ws(<<$m, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$m>>);
decode_cookie_av_ws(<<$M, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$m>>);
decode_cookie_av_ws(<<$p, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$p>>);
decode_cookie_av_ws(<<$P, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$p>>);
decode_cookie_av_ws(<<$d, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$d>>);
decode_cookie_av_ws(<<$D, Rest/bits>>, C) ->
    decode_cookie_av(Rest, C, <<$d>>);
decode_cookie_av_ws(Rest, C) ->
    ignore_cookie_av(Rest, C).

ignore_cookie_av(<<$;, Rest/bits>>, Co) ->
    decode_cookie_av_ws(Rest, Co);
ignore_cookie_av(<<_, Rest/bits>>, Co) ->
    ignore_cookie_av(Rest, Co);
ignore_cookie_av(<<>>, Co) ->
    Co.

%% Match only uppercase chars on Expires, Max-Age, Path, Domain
decode_cookie_av(<<$=, Rest/bits>>, Co, AV) ->
    decode_cookie_av_value(Rest, Co, AV, <<>>);
decode_cookie_av(<<$D, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $d>>);
decode_cookie_av(<<$O, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $o>>);
decode_cookie_av(<<$N, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $n>>);
decode_cookie_av(<<$E, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $e>>);
decode_cookie_av(<<$X, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $x>>);
decode_cookie_av(<<$P, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $p>>);
decode_cookie_av(<<$I, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $i>>);
decode_cookie_av(<<$R, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $r>>);
decode_cookie_av(<<$S, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $s>>);
decode_cookie_av(<<$M, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $m>>);
decode_cookie_av(<<$A, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $a>>);
decode_cookie_av(<<$G, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $g>>);
decode_cookie_av(<<$T, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $t>>);
decode_cookie_av(<<$H, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, $h>>);
decode_cookie_av(<<$;, Rest/bits>>, Co, _AV) ->
    decode_cookie_av_ws(Rest, Co);
decode_cookie_av(<<C, Rest/bits>>, Co, AV) ->
    decode_cookie_av(Rest, Co, <<AV/binary, C>>);
decode_cookie_av(<<>>, Co, _AV) ->
    ignore_cookie_av(<<>>, Co).

decode_cookie_av_value(<<>>, Co, <<"path">>, Value) ->
    Co#fusco_cookie{path_tokens = binary:split(Value, <<"/">>, [global]),
                    path = Value};
decode_cookie_av_value(<<>>, Co, <<"max-age">>, Value) ->
    Co#fusco_cookie{max_age = max_age(Value)};
decode_cookie_av_value(<<>>, Co, <<"expires">>, Value) ->
    Co#fusco_cookie{expires = expires(Value)};
decode_cookie_av_value(<<>>, Co, <<"domain">>, Value) ->
    Co#fusco_cookie{domain = Value};
decode_cookie_av_value(<<$;, Rest/bits>>, Co, <<"path">>, Value) ->
    Path = binary:split(Value, <<"/">>, [global]),
    decode_cookie_av_ws(Rest, Co#fusco_cookie{path_tokens = Path,
                                              path = Value});
decode_cookie_av_value(<<$;, Rest/bits>>, Co, <<"max-age">>, Value) ->
    decode_cookie_av_ws(Rest, Co#fusco_cookie{
                max_age = max_age(Value)});
decode_cookie_av_value(<<$;, Rest/bits>>, Co, <<"expires">>, Value) ->
    %% TODO parse expires
    decode_cookie_av_ws(Rest, Co#fusco_cookie{expires = expires(Value)});
decode_cookie_av_value(<<$;, Rest/bits>>, Co, <<"domain">>, Value) ->
    decode_cookie_av_ws(Rest, Co#fusco_cookie{domain = Value});
decode_cookie_av_value(<<$;, Rest/bits>>, Co, _, _) ->
    decode_cookie_av_ws(Rest, Co);
decode_cookie_av_value(<<C, Rest/bits>>, Co, AV, Value) ->
    decode_cookie_av_value(Rest, Co, AV, <<Value/binary, C>>).


decode_body(<<>>, Response = #response{status_code = <<$1, _, _>>,
                       transfer_encoding = TE}, _Timeout)
  when TE =/= <<"chunked">> ->
    return(<<>>, Response);
decode_body(<<$\r, $\n, Rest/bits>>, Response, Timeout) ->
    decode_body(Rest, Response, Timeout);
decode_body(Rest, Response = #response{status_code = <<$1, _, _>>,
                       transfer_encoding = TE}, Timeout)
  when TE =/= <<"chunked">> ->
    decode_status_line(Rest, #response{socket = Response#response.socket,
                       ssl = Response#response.ssl,
                       in_timestamp = Response#response.in_timestamp}, Timeout);
decode_body(Rest, Response = #response{transfer_encoding = <<"chunked">>},
            Timeout) ->
    decode_chunked_body(Rest, <<>>, <<>>, Response, Timeout);
decode_body(Rest, Response, Timeout) ->
    case byte_size(Rest) >= Response#response.content_length of
    true ->
        return(Rest, Response);
    false ->
        case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                             Timeout) of
        {ok, Data} ->
            decode_body(<<Rest/binary, Data/binary>>, ?SIZE(Data, Response),
                        Timeout);
        _ ->
            %% NOTE: Return what we have so far
            return(Rest, Response)
        end
    end.

download_chunked_body(Rest, Acc, Size, Response, Timeout) ->
    case fusco_sock:recv(Response#response.socket, Response#response.ssl,
                         Timeout) of
    {ok, Data} ->
        decode_chunked_body(<<Rest/bits, Data/bits>>, Acc, Size,
                ?SIZE(Data, Response), Timeout);
    _ ->
        return(Acc, Response)
    end.

decode_chunked_body(<<$0,$\r,$\n,$\r,$\n>>, Acc, _, Response, _Timeout) ->
    return(Acc, Response);
decode_chunked_body(<<$0, Rest/bits>> = R, Acc, Size, Response, Timeout)
  when is_binary(Size),    byte_size(Rest) < 4 ->
    download_chunked_body(R, Acc, Size, Response, Timeout);
decode_chunked_body(<<$\r>> = R, Acc, Size, Response,
                    Timeout) when is_binary(Size) ->
    download_chunked_body(R, Acc, Size, Response, Timeout);
decode_chunked_body(<<$\r,$\n, Rest/bits>>, Acc, <<>>, Response, Timeout) ->
    decode_chunked_body(Rest, Acc, <<>>, Response, Timeout);
decode_chunked_body(<<$\r,$\n, Rest/bits>>, Acc, Size, Response,
                    Timeout) when is_binary(Size) ->
    IntSize = erlang:binary_to_integer(Size, 16),
    decode_chunked_body(Rest, Acc, IntSize, Response, Timeout);
decode_chunked_body(<<C, Rest/bits>>, Acc, Size, Response,
                    Timeout) when is_binary(Size) ->
    decode_chunked_body(Rest, Acc, <<Size/bits, C>>, Response, Timeout);
decode_chunked_body(<<>> = R, Acc, Size, Response,
                    Timeout) when is_binary(Size) ->
    download_chunked_body(R, Acc, Size, Response, Timeout);
decode_chunked_body(Rest, Acc, Size, Response, Timeout) when is_integer(Size) ->
    case byte_size(Rest) of
    S when S == Size ->
        decode_chunked_body(<<>>, <<Acc/bits, Rest/bits>>, <<>>, Response,
                            Timeout);
    S when S < Size ->
        download_chunked_body(Rest, Acc, Size, Response, Timeout);
    S when S > Size ->
        Current = binary:part(Rest, 0, Size),
        Next = binary:part(Rest, Size, S - Size),
        decode_chunked_body(Next, <<Acc/bits, Current/bits>>, <<>>, Response,
                            Timeout)
    end.

return(Body, Response) ->
    Response#response{body = Body}.

max_age(Value) ->
    binary_to_integer(Value) * 1000000.

%% http://tools.ietf.org/html/rfc2616#section-3.3.1
%% Supports some non-standard datetime (Tomcat) Tue, 06-Nov-1994 08:49:37 GMT
expires(<<_,_,_,$,,$\s,D1,D2,$\s,M1,M2,M3,$\s,Y1,Y2,Y3,Y4,$\s,Rest/bits>>) ->
    expires(Rest, {list_to_integer([Y1,Y2,Y3,Y4]),month(<<M1,M2,M3>>),
                   list_to_integer([D1,D2])});
expires(<<_,_,_,$\s,Mo1,Mo2,Mo3,$\s,D1,D2,$\s,H1,H2,$:,M1,M2,$:,S1,S2,$\s,
        Y1,Y2,Y3,Y4,_Rest/bits>>) ->
    { { list_to_integer([Y1,Y2,Y3,Y4]),
        month(<<Mo1,Mo2,Mo3>>),
        list_to_integer([D1,D2])
      },
      { list_to_integer([H1,H2]),
        list_to_integer([M1,M2]),
        list_to_integer([S1,S2])
    } };
expires(<<_,_,_,$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Monday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Tuesday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Wednesday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Thursday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Friday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Saturday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<"Sunday",$,,$\s,Rest/bits>>) ->
    expires(Rest);
expires(<<D1,D2,$\-,M1,M2,M3,$\-,Y1,Y2,Y3,Y4,$\s,Rest/bits>>) ->
    expires(Rest, {list_to_integer([Y1,Y2,Y3,Y4]),month(<<M1,M2,M3>>),
                   list_to_integer([D1,D2])});
expires(<<D1,D2,$\-,M1,M2,M3,$\-,Y3,Y4,$\s,Rest/bits>>) ->
    %% http://tools.ietf.org/html/rfc2616#section-19.3
    %% HTTP/1.1 clients and caches SHOULD assume that an RFC-850 date
    %% which appears to be more than 50 years in the future is in fact
    %% in the past (this helps solve the "year 2000" problem).
    expires(Rest, {to_year([Y3, Y4]),month(<<M1,M2,M3>>),
                   list_to_integer([D1,D2])}).

to_year(List) ->
    Int = list_to_integer(List),
    {Y, _, _} = date(),
    case (2000 + Int - Y) > 50 of
        true ->
            1900 + Int;
        false ->
            2000 + Int
    end.

expires(<<H1,H2,$:,M1,M2,$:,S1,S2,_Rest/bits>>, Date) ->
    {Date, {list_to_integer([H1,H2]), list_to_integer([M1,M2]),
            list_to_integer([S1,S2])}}.

month(<<$J,$a,$n>>) ->
    1;
month(<<$F,$e,$b>>) ->
    2;
month(<<$M,$a,$r>>) ->
    3;
month(<<$A,$p,$r>>) ->
    4;
month(<<$M,$a,$y>>) ->
    5;
month(<<$J,$u,$n>>) ->
    6;
month(<<$J,$u,$l>>) ->
    7;
month(<<$A,$u,$g>>) ->
    8;
month(<<$S,$e,$p>>) ->
    9;
month(<<$O,$c,$t>>) ->
    10;
month(<<$N,$o,$v>>) ->
    11;
month(<<$D,$e,$c>>) ->
    12.
