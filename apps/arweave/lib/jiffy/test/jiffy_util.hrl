
-compile(export_all).

msg(Fmt, Args) ->
    M1 = io_lib:format(Fmt, Args),
    M2 = re:replace(M1, <<"\r">>, <<"\\\\r">>, [global]),
    M3 = re:replace(M2, <<"\n">>, <<"\\\\n">>, [global]),
    M4 = re:replace(M3, <<"\t">>, <<"\\\\t">>, [global]),
    iolist_to_binary(M4).


hex(Bin) when is_binary(Bin) ->
    H1 = [io_lib:format("16#~2.16.0B",[X]) || <<X:8>> <= Bin],
    H2 = string:join(H1, ", "),
    lists:flatten(io_lib:format("<<~s>>", [lists:flatten(H2)])).


dec(V) ->
    jiffy:decode(V).


enc(V) ->
    iolist_to_binary(jiffy:encode(V)).


enc(V, Opts) ->
    iolist_to_binary(jiffy:encode(V, Opts)).


%% rebar runs eunit with PWD as .eunit/
%% rebar3 runs eunit with PWD as ./
%% this adapts to the differences
cases_path(Suffix) ->
    {ok, Cwd} = file:get_cwd(),
    Prefix = case filename:basename(Cwd) of
        ".eunit" -> "..";
        _ -> "."
    end,
    Path = "test/cases",
    filename:join([Prefix, Path, Suffix]).
