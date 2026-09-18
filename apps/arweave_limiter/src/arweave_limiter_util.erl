-module(arweave_limiter_util).

-export([worker_name/2,
         worker_ref/3]).


worker_name(LimiterRef, WorkerNum) when is_atom(LimiterRef) ->
    list_to_atom(lists:flatten(io_lib:format("arweave_limiter_~p_~p", [LimiterRef, WorkerNum]))).

worker_ref(LimiterRef, {A, B, C, D, _P}, NumberOfWorkers) ->
    worker_ref(LimiterRef, {A,B,C,D}, NumberOfWorkers);
worker_ref(LimiterRef, {_A,_B,_C,D}, NumberOfWorkers) when is_atom(LimiterRef),
                                                     is_integer(D),
                                                     is_integer(NumberOfWorkers) ->
    WorkerNum = D rem NumberOfWorkers,
    worker_name(LimiterRef, WorkerNum).
