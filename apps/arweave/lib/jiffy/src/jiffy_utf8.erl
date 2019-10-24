% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_utf8).
-export([fix/1]).


fix({Props}) ->
    fix_props(Props, []);
fix(Values) when is_list(Values) ->
    fix_array(Values, []);
fix(Bin) when is_binary(Bin) ->
    fix_bin(Bin);
fix(Val) ->
    maybe_map(Val).

-ifndef(JIFFY_NO_MAPS).
maybe_map(Obj) when is_map(Obj) ->
    maps:fold(fun fix_map/3, maps:new(), Obj);
maybe_map(Val) ->
    Val.

fix_map(K, V, Acc) ->
    maps:put(fix(K), fix(V), Acc).
-else.
maybe_map(Val) ->
    Val.
-endif.

fix_props([], Acc) ->
    {lists:reverse(Acc)};
fix_props([{K0, V0} | Rest], Acc) ->
    K = fix(K0),
    V = fix(V0),
    fix_props(Rest, [{K, V} | Acc]).


fix_array([], Acc) ->
    lists:reverse(Acc);
fix_array([Val | Rest], Acc0) ->
    Acc = [fix(Val) | Acc0],
    fix_array(Rest, Acc).


fix_bin(Bin) ->
    Dec0 = loose_decode(Bin, 0, []),
    Dec1 = try_combining(Dec0, []),
    Dec2 = replace_garbage(Dec1, []),
    list_to_binary(xmerl_ucs:to_utf8(Dec2)).


loose_decode(Bin, O, Acc) ->
    case Bin of
        <<_:O/binary>> ->
            lists:reverse(Acc);
        <<_:O/binary, 0:1/integer, V:7/integer, _/binary>> ->
            loose_decode(Bin, O+1, [V | Acc]);
        <<_:O/binary, 6:3/integer, V0:5/integer,
                2:2/integer, V1:6/integer, _/binary>> ->
            B = <<0:5/integer, V0:5/integer, V1:6/integer>>,
            <<V:16/integer>> = B,
            loose_decode(Bin, O+2, [V | Acc]);
        <<_:O/binary, 14:4/integer, V0:4/integer,
                2:2/integer, V1:6/integer,
                2:2/integer, V2:6/integer, _/binary>> ->
            B = <<V0:4/integer, V1:6/integer, V2:6/integer>>,
            <<V:16/integer>> = B,
            loose_decode(Bin, O+3, [V | Acc]);
        <<_:O/binary, 30:5/integer, V0:3/integer,
                2:2/integer, V1:6/integer,
                2:2/integer, V2:6/integer,
                2:2/integer, V3:6/integer, _/binary>> ->
            B = <<0:11/integer, V0:3/integer, V1:6/integer,
                    V2:6/integer, V3:6/integer>>,
            <<V:32/integer>> = B,
            loose_decode(Bin, O+4, [V | Acc]);
        <<_:O/binary, _:8/integer, R/binary>> ->
            % Broken lead or continuation byte. Discard first
            % byte and all broken continuations. Replace the
            % whole mess with a replacment code point.
            T = 1 + count_continuation_bytes(R, 0),
            loose_decode(Bin, O+T, [16#FFFD | Acc])
    end.


count_continuation_bytes(R, O) ->
    case R of
        <<_:O/binary, 2:2/integer, _:6/integer, _/binary>> ->
            count_continuation_bytes(R, O+1);
        _ ->
            O
    end.


try_combining([], Acc) ->
    lists:reverse(Acc);
try_combining([H, L | Rest], Acc) when H >= 16#D800, H =< 16#DFFF,
                                        L >= 16#D800, L =< 16#DFFF ->
    Bin = <<H:16/big-unsigned-integer, L:16/big-unsigned-integer>>,
    try
        [C] = xmerl_ucs:from_utf16be(Bin),
        try_combining(Rest, [C | Acc])
    catch _:_ ->
        try_combining(Rest, [L, H | Acc])
    end;
try_combining([C | Rest], Acc) ->
    try_combining(Rest, [C | Acc]).


replace_garbage([], Acc) ->
    lists:reverse(Acc);
replace_garbage([C | Rest], Acc) ->
    case xmerl_ucs:is_unicode(C) of
        true -> replace_garbage(Rest, [C | Acc]);
        false -> replace_garbage(Rest, [16#FFFD | Acc])
    end.
