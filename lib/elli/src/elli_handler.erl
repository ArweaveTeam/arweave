-module(elli_handler).
-include("elli.hrl").

-callback handle(Req :: #req{}, callback_args()) ->
    ignore | {response_code(), [tuple()], binary()} | {ok, [tuple()], binary()}.
-callback handle_event(Event :: elli_event(), Args :: [term()], Config :: [tuple()]) -> ok.
