-module(arweave_limiter_util).
 
-export([worker_name/2,
         worker_ref/3]).

-include_lib("eunit/include/eunit.hrl").

worker_name(LimiterRef, WorkerNum) when is_atom(LimiterRef) ->
    list_to_atom(lists:flatten(io_lib:format("arweave_limiter_~p_~p", [LimiterRef, WorkerNum]))).

worker_ref(LimiterRef, {A, B, C, D, _P}, NumberOfWorkers) ->
    worker_ref(LimiterRef, {A,B,C,D}, NumberOfWorkers);
worker_ref(LimiterRef, {_A,_B,_C,D}, NumberOfWorkers) when is_atom(LimiterRef),
                                                     is_integer(D),
                                                     is_integer(NumberOfWorkers) ->
    WorkerNum = D rem NumberOfWorkers,
    worker_name(LimiterRef, WorkerNum).

%%% TEST
name_test() ->
    ?assertEqual('arweave_limiter_test_limiter_0', worker_name(test_limiter, 0)),
    ok.

worker_ref_test() ->
    ?assertEqual('arweave_limiter_test_limiter_0', worker_ref(test_limiter,{1,2,3,4},1)),
    ?assertEqual('arweave_limiter_test_limiter_0', worker_ref(test_limiter,{1,2,3,0},5)),
    ?assertEqual('arweave_limiter_test_limiter_1', worker_ref(test_limiter,{1,2,3,1},5)),
    ?assertEqual('arweave_limiter_test_limiter_2', worker_ref(test_limiter,{1,2,3,2},5)),
    ?assertEqual('arweave_limiter_test_limiter_3', worker_ref(test_limiter,{1,2,3,3},5)),
    ?assertEqual('arweave_limiter_test_limiter_4', worker_ref(test_limiter,{1,2,3,4},5)),
    ?assertEqual('arweave_limiter_test_limiter_0', worker_ref(test_limiter,{1,2,3,5},5)),
    ok.
