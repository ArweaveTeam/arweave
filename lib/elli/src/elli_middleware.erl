%% @doc: HTTP request processing middleware.
%%
%% This module offers both pre-processing of requests and
%% post-processing of responses. It can also be used to allow multiple
%% handlers, where the first handler to return a response
%% short-circuits the request. It is implemented as a plain elli
%% handler.
%%
%% Usage:
%%  Config = [
%%            {mods, [
%%                    {elli_example_middleware, []},
%%                    {elli_middleware_compress, []},
%%                    {elli_example_callback, []}
%%                   ]}
%%             ],
%%  elli:start_link([..., {callback, elli_middleware}, {callback_args, Config}]).
%%
%% The configured modules may implement the elli behaviour, in which
%% case all the callbacks will be used as normal. If handle/2 returns
%% 'ignore', elli will continue on to the next callback in the list.
%%
%% Pre-processing and post-processing is implemented in preprocess/2
%% and postprocess/3. preprocess/2 is called for each middleware in
%% the order specified, while postprocess/3 is called in the reverse
%% order. TODO: Even if a middleware short-circuits the request, all
%% postprocess middlewares will be called.
%%
%% elli_middleware does not add any significant overhead.

-module(elli_middleware).
-behaviour(elli_handler).
-export([init/2, handle/2, handle_event/3]).

%%
%% ELLI CALLBACKS
%%

init(Req, Args) ->
    do_init(Req, mods(Args)).


handle(CleanReq, Config) ->
    Mods   = mods(Config),
    PreReq = preprocess(CleanReq, Mods),
    Res    = process(PreReq, Mods),
    postprocess(PreReq, Res, lists:reverse(Mods)).


handle_event(elli_startup, Args, Config) ->
    lists:foreach(fun code:ensure_loaded/1, lists:map(fun ({M, _}) -> M end,
                                                  mods(Config))),
    forward_event(elli_startup, Args, mods(Config));
handle_event(Event, Args, Config) ->
    forward_event(Event, Args, lists:reverse(mods(Config))).




%%
%% MIDDLEWARE LOGIC
%%

do_init(_, []) ->
    {ok, standard};
do_init(Req, [{Mod, Args}|Mods]) ->
    case erlang:function_exported(Mod, init, 2) of
        true ->
            case Mod:init(Req, Args) of
                ignore ->
                    do_init(Req, Mods);
                Result ->
                    Result
            end;
        false ->
            do_init(Req, Mods)
    end.


process(_Req, []) ->
    {404, [], <<"Not Found">>};
process(Req, [{Mod, Args} | Mods]) ->
    case erlang:function_exported(Mod, handle, 2) of
        true ->
            case Mod:handle(Req, Args) of
                ignore ->
                    process(Req, Mods);
                Response ->
                    Response
            end;
        false ->
            process(Req, Mods)
    end.

preprocess(Req, []) ->
    Req;
preprocess(Req, [{Mod, Args} | Mods]) ->
    case erlang:function_exported(Mod, preprocess, 2) of
        true ->
            preprocess(Mod:preprocess(Req, Args), Mods);
        false ->
            preprocess(Req, Mods)
    end.

postprocess(_Req, Res, []) ->
    Res;
postprocess(Req, Res, [{Mod, Args} | Mods]) ->
    case erlang:function_exported(Mod, postprocess, 3) of
        true ->
            postprocess(Req, Mod:postprocess(Req, Res, Args), Mods);
        false ->
            postprocess(Req, Res, Mods)
    end.


forward_event(Event, Args, Mods) ->
    lists:foreach(
      fun ({M, ExtraArgs}) ->
              case erlang:function_exported(M, handle_event, 3) of
                  true ->
                      M:handle_event(Event, Args, ExtraArgs);
                  false ->
                      ok
              end
      end, Mods),
    ok.


%%
%% INTERNAL HELPERS
%%

mods(Config) -> proplists:get_value(mods, Config).
