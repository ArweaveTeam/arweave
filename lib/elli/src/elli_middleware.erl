%%% @doc HTTP request processing middleware.
%%%
%%% This module offers both pre-processing of requests and post-processing of
%%% responses. It can also be used to allow multiple handlers, where the first
%%% handler to return a response short-circuits the request.
%%% It is implemented as a plain elli handler.
%%%
%%% Usage:
%%%
%%% ```
%%% Config = [
%%%           {mods, [
%%%                   {elli_example_middleware, []},
%%%                   {elli_middleware_compress, []},
%%%                   {elli_example_callback, []}
%%%                  ]}
%%%          ],
%%% elli:start_link([
%%%                  %% ...,
%%%                  {callback, elli_middleware},
%%%                  {callback_args, Config}
%%%                 ]).
%%% '''
%%%
%%% The configured modules may implement the elli behaviour, in which case all
%%% the callbacks will be used as normal. If {@link handle/2} returns `ignore',
%%% elli will continue on to the next callback in the list.
%%%
%%% Pre-processing and post-processing is implemented in {@link preprocess/2}
%%% and {@link postprocess/3}. {@link preprocess/2} is called for each
%%% middleware in the order specified, while {@link postprocess/3} is called in
%%% the reverse order.
%%%
%%% TODO: Don't call all postprocess middlewares when a middleware
%%% short-circuits the request.
%%%
%%% `elli_middleware' does not add any significant overhead.

-module(elli_middleware).
-behaviour(elli_handler).
-export([init/2, handle/2, handle_event/3]).

%% Macros.
-define(IF_NOT_EXPORTED(M, F, A, T, E),
        case erlang:function_exported(M, F, A) of true -> E; false -> T end).

%%
%% ELLI CALLBACKS
%%

%% @hidden
-spec init(Req, Args) -> {ok, standard | handover} when
      Req  :: elli:req(),
      Args :: elli_handler:callback_args().
init(Req, Args) ->
    do_init(Req, callbacks(Args)).


%% @hidden
-spec handle(Req :: elli:req(), Config :: [tuple()]) -> elli_handler:result().
handle(CleanReq, Config) ->
    Callbacks = callbacks(Config),
    PreReq    = preprocess(CleanReq, Callbacks),
    Res       = process(PreReq, Callbacks),
    postprocess(PreReq, Res, lists:reverse(Callbacks)).


%% @hidden
-spec handle_event(Event, Args, Config) -> ok when
      Event  :: elli_handler:event(),
      Args   :: elli_handler:callback_args(),
      Config :: [tuple()].
handle_event(elli_startup, Args, Config) ->
    Callbacks = callbacks(Config),
    [code:ensure_loaded(M) || {M, _} <- Callbacks],
    forward_event(elli_startup, Args, Callbacks);
handle_event(Event, Args, Config) ->
    forward_event(Event, Args, lists:reverse(callbacks(Config))).




%%
%% MIDDLEWARE LOGIC
%%

-spec do_init(Req, Callbacks) -> {ok, standard | handover} when
      Req       :: elli:req(),
      Callbacks :: elli_handler:callbacks().
do_init(_, []) ->
    {ok, standard};
do_init(Req, [{Mod, Args}|Mods]) ->
    ?IF_NOT_EXPORTED(Mod, init, 2, do_init(Req, Mods),
                     case Mod:init(Req, Args) of
                         ignore -> do_init(Req, Mods);
                         Result -> Result
                     end).


-spec process(Req, Callbacks) -> Result when
      Req       :: elli:req(),
      Callbacks :: [Callback :: elli_handler:callback()],
      Result    :: elli_handler:result().
process(_Req, []) ->
    {404, [], <<"Not Found">>};
process(Req, [{Mod, Args} | Mods]) ->
    ?IF_NOT_EXPORTED(Mod, handle, 2, process(Req, Mods),
                     case Mod:handle(Req, Args) of
                         ignore   -> process(Req, Mods);
                         Response -> Response
                     end).

-spec preprocess(Req1, Callbacks) -> Req2 when
      Req1      :: elli:req(),
      Callbacks :: [elli_handler:callback()],
      Req2      :: elli:req().
preprocess(Req, []) ->
    Req;
preprocess(Req, [{Mod, Args} | Mods]) ->
    ?IF_NOT_EXPORTED(Mod, preprocess, 2, preprocess(Req, Mods),
                     preprocess(Mod:preprocess(Req, Args), Mods)).

-spec postprocess(Req, Res1, Callbacks) -> Res2 when
      Req       :: elli:req(),
      Res1      :: elli_handler:result(),
      Callbacks :: [elli_handler:callback()],
      Res2      :: elli_handler:result().
postprocess(_Req, Res, []) ->
    Res;
postprocess(Req, Res, [{Mod, Args} | Mods]) ->
    ?IF_NOT_EXPORTED(Mod, postprocess, 3, postprocess(Req, Res, Mods),
                     postprocess(Req, Mod:postprocess(Req, Res, Args), Mods)).


-spec forward_event(Event, Args, Callbacks) -> ok when
      Event     :: elli_handler:event(),
      Args      :: elli_handler:callback_args(),
      Callbacks :: [elli_handler:callback()].
forward_event(Event, Args, Callbacks) ->
    [?IF_NOT_EXPORTED(M, handle_event, 3, ok,
                      M:handle_event(Event, Args, XArgs))
     || {M, XArgs} <- Callbacks],
    ok.


%%
%% INTERNAL HELPERS
%%

-spec callbacks(Config :: [{mod, Callbacks} | tuple()]) -> Callbacks when
      Callbacks :: [elli_handler:callback()].
callbacks(Config) ->
    proplists:get_value(mods, Config, []).
