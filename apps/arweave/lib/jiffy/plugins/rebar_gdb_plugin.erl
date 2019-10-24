-module(rebar_gdb_plugin).
-compile(export_all).

pre_eunit(_Config, _AppFile) ->
    case os:getenv("USE_GDB") of
        false ->
            ok;
        _ ->
            Prompt = io_lib:format("GDB Attach to: ~s~n", [os:getpid()]),
            io:get_line(Prompt)
    end,
    ok.

