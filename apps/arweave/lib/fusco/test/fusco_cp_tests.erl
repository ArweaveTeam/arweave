%%% ----------------------------------------------------------------------------
%%% @copyright (C) 2013, Erlang Solutions Ltd
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc Fusco Client Pool tests
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco_cp_tests).

-include_lib("eunit/include/eunit.hrl").

-define(POOL, fusco_pool).

client_pool_test_() ->
    {foreach,
     fun() ->
             {ok, Pid} = fusco_cp:start({"127.0.0.1", 5050, false}, [], 3),
             erlang:register(?POOL, Pid),
             Pid
     end,
     fun(Pid) ->
             fusco_cp:stop(Pid)
     end,
     [
      {timeout, 60000, {"Get client", fun get_client/0}},
      {"Free client", fun free_client/0},
      {"Unblock client", fun unblock_client/0}
     ]
    }.

get_client() ->
    ?assertEqual(true, is_pid(fusco_cp:get_client(?POOL))),
    ?assertEqual(true, is_pid(fusco_cp:get_client(?POOL))),
    ?assertEqual(true, is_pid(fusco_cp:get_client(?POOL))),
    ?assertEqual({error, timeout}, fusco_cp:get_client(?POOL)).

free_client() ->
    Pid = fusco_cp:get_client(?POOL),
    ?assertEqual(true, is_pid(Pid)),
    ?assertEqual(ok, fusco_cp:free_client(?POOL, Pid)),
    ?assertEqual(Pid, fusco_cp:get_client(?POOL)).

unblock_client() ->
    Client = fusco_cp:get_client(?POOL),
    ?assertEqual(true, is_pid(Client)),
    ?assertEqual(true, is_pid(fusco_cp:get_client(?POOL))),
    ?assertEqual(true, is_pid(fusco_cp:get_client(?POOL))),
    To = self(),
    spawn(fun() ->
                  Pid = fusco_cp:get_client(?POOL),
                  To ! {client, Pid}
          end),
    ?assertEqual(ok, fusco_cp:free_client(?POOL, Client)),
    ?assertEqual({client, Client}, receive
                                       {client, _} = R ->
                                           R
                                   end).
