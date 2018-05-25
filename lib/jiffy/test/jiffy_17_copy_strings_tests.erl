% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_17_copy_strings_tests).

-include_lib("eunit/include/eunit.hrl").


check_binaries({Props}) when is_list(Props) ->
    lists:all(fun({Key, Value}) ->
        check_binaries(Key) andalso check_binaries(Value)
    end, Props);
check_binaries(Values) when is_list(Values) ->
    lists:all(fun(Value) ->
        check_binaries(Value)
    end, Values);
check_binaries(Bin) when is_binary(Bin) ->
    io:format("~s :: ~p ~p", [Bin, byte_size(Bin), binary:referenced_byte_size(Bin)]),
    byte_size(Bin) == binary:referenced_byte_size(Bin);
check_binaries(Bin) ->
    true.


copy_strings_test_() ->
    Opts = [copy_strings],
    Cases = [
        <<"\"foo\"">>,
        <<"[\"bar\"]">>,
        <<"{\"foo\":\"bar\"}">>,
        <<"{\"foo\":[\"bar\"]}">>
    ],
    {"Test copy_strings", lists:map(fun(Json) ->
        EJson = jiffy:decode(Json, Opts),
        ?_assert(check_binaries(EJson))
    end, Cases)}.
