%%
%% @doc URL safe base64-compatible codec.
%%
%% Based heavily on the code extracted from:
%%   https://github.com/basho/riak_control/blob/master/src/base64url.erl and
%%   https://github.com/mochi/mochiweb/blob/master/src/mochiweb_base64url.erl.
%%

-module(base64url).
-author('Vladimir Dronnikov <dronnikov@gmail.com>').

-export([
    decode/1,
    encode/1,
    encode_mime/1
  ]).

-spec encode(
    binary() | iolist()
  ) -> binary().

encode(Bin) when is_binary(Bin) ->
  << << (urlencode_digit(D)) >> || <<D>> <= base64:encode(Bin), D =/= $= >>;
encode(L) when is_list(L) ->
  encode(iolist_to_binary(L)).

-spec encode_mime(
    binary() | iolist()
  ) -> binary().
encode_mime(Bin) when is_binary(Bin) ->
    << << (urlencode_digit(D)) >> || <<D>> <= base64:encode(Bin) >>;
encode_mime(L) when is_list(L) ->
    encode_mime(iolist_to_binary(L)).

-spec decode(
    binary() | iolist()
  ) -> binary().

decode(Bin) when is_binary(Bin) ->
  Bin2 = case byte_size(Bin) rem 4 of
    % 1 -> << Bin/binary, "===" >>;
    2 -> << Bin/binary, "==" >>;
    3 -> << Bin/binary, "=" >>;
    _ -> Bin
  end,
  base64:decode(<< << (urldecode_digit(D)) >> || <<D>> <= Bin2 >>);
decode(L) when is_list(L) ->
  decode(iolist_to_binary(L)).

urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

aim_test() ->
  % vanilla base64 produce URL unsafe output
  ?assertNotEqual(
      binary:match(base64:encode([255,127,254,252]), [<<"=">>, <<"/">>, <<"+">>]),
      nomatch),
  % this codec produce URL safe output
  ?assertEqual(
      binary:match(encode([255,127,254,252]), [<<"=">>, <<"/">>, <<"+">>]),
      nomatch),
  % the mime codec produces URL unsafe output, but only because of padding
  ?assertEqual(
      binary:match(encode_mime([255,127,254,252]), [<<"/">>, <<"+">>]),
      nomatch),
  ?assertNotEqual(
      binary:match(encode_mime([255,127,254,252]), [<<"=">>]),
      nomatch).

codec_test() ->
  % codec is lossless with or without padding
  ?assertEqual(decode(encode(<<"foo">>)), <<"foo">>),
  ?assertEqual(decode(encode(<<"foo1">>)), <<"foo1">>),
  ?assertEqual(decode(encode(<<"foo12">>)), <<"foo12">>),
  ?assertEqual(decode(encode(<<"foo123">>)), <<"foo123">>),
  ?assertEqual(decode(encode_mime(<<"foo">>)), <<"foo">>),
  ?assertEqual(decode(encode_mime(<<"foo1">>)), <<"foo1">>),
  ?assertEqual(decode(encode_mime(<<"foo12">>)), <<"foo12">>),
  ?assertEqual(decode(encode_mime(<<"foo123">>)), <<"foo123">>).

iolist_test() ->
  % codec supports iolists
  ?assertEqual(decode(encode("foo")), <<"foo">>),
  ?assertEqual(decode(encode(["fo", "o1"])), <<"foo1">>),
  ?assertEqual(decode(encode([255,127,254,252])), <<255,127,254,252>>),
  ?assertEqual(decode(encode_mime("foo")), <<"foo">>),
  ?assertEqual(decode(encode_mime(["fo", "o1"])), <<"foo1">>),
  ?assertEqual(decode(encode_mime([255,127,254,252])), <<255,127,254,252>>).

-endif.
