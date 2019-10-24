% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_01_yajl_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


yajl_test_() ->
    Cases = read_cases(),
    [gen(Case) || Case <- Cases].


gen({Name, Json, {error, _}=Erl}) ->
    {Name, ?_assertThrow(Erl, jiffy:decode(Json))};
gen({Name, Json, Erl}) ->
    {Name, ?_assertEqual(Erl, jiffy:decode(Json))}.


read_cases() ->
    CasesPath = cases_path("*.json"),
    FileNames = lists:sort(filelib:wildcard(CasesPath)),
    lists:map(fun(F) -> make_pair(F) end, FileNames).


make_pair(FileName) ->
    {ok, Json} = file:read_file(FileName),
    BaseName = filename:rootname(FileName),
    ErlFname = BaseName ++ ".eterm",
    {ok, [Term]} = file:consult(ErlFname),
    {filename:basename(BaseName), Json, Term}.
