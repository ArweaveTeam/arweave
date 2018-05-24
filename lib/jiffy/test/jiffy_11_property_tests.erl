% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_11_property_tests).

-ifdef(HAVE_EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


property_test_() ->
    [
        run(prop_enc_dec),
        run(prop_enc_dec_pretty),
        run(prop_dec_trailer),
        run(prop_enc_no_crash),
        run(prop_dec_no_crash_bin),
        run(prop_dec_no_crash_any)
    ] ++ map_props().


-ifndef(JIFFY_NO_MAPS).
map_props() ->
    [
        run(prop_map_enc_dec)
    ].
-else.
map_props() ->
    [].
-endif.


prop_enc_dec() ->
    ?FORALL(Data, json(), begin
        Data == jiffy:decode(jiffy:encode(Data))
    end).


prop_dec_trailer() ->
    ?FORALL({T1, Comb, T2}, {json(), combiner(), json()},
        begin
            B1 = jiffy:encode(T1),
            B2 = jiffy:encode(T2),
            Bin = <<B1/binary, Comb/binary, B2/binary>>,
            {has_trailer, T1, Rest} = jiffy:decode(Bin, [return_trailer]),
            T2 = jiffy:decode(Rest),
            true
        end
    ).


prop_enc_dec_pretty() ->
    ?FORALL(Data, json(),
        begin
            Data == jiffy:decode(jiffy:encode(Data, [pretty]))
        end
    ).


-ifndef(JIFFY_NO_MAPS).
prop_map_enc_dec() ->
    ?FORALL(Data, json(),
        begin
            MapData = to_map_ejson(Data),
            MapData == jiffy:decode(jiffy:encode(MapData), [return_maps])
        end
    ).
-endif.


prop_enc_no_crash() ->
    ?FORALL(Data, any(), begin catch jiffy:encode(Data), true end).


prop_dec_no_crash_any() ->
    ?FORALL(Data, any(), begin catch jiffy:decode(Data), true end).


prop_dec_no_crash_bin() ->
    ?FORALL(Data, binary(), begin catch jiffy:decode(Data), true end).


opts() ->
    [
        {numtests, [1000]}
    ].


apply_opts(Prop) ->
    apply_opts(Prop, opts()).


apply_opts(Prop, []) ->
    Prop;

apply_opts(Prop, [{Name, Args} | Rest]) ->
    NewProp = erlang:apply(eqc, Name, Args ++ [Prop]),
    apply_opts(NewProp, Rest).


log(F, A) ->
    io:format(standard_error, F, A).


run(Name) ->
    Prop = apply_opts(?MODULE:Name()),
    {msg("~s", [Name]), [
        {timeout, 300, ?_assert(eqc:quickcheck(Prop))}
    ]}.


-ifndef(JIFFY_NO_MAPS).
to_map_ejson({Props}) ->
    NewProps = [{K, to_map_ejson(V)} || {K, V} <- Props],
    maps:from_list(NewProps);
to_map_ejson(Vals) when is_list(Vals) ->
    [to_map_ejson(V) || V <- Vals];
to_map_ejson(Val) ->
    Val.
-endif.


% Random any term generation

any() ->
    ?SIZED(Size, any(Size)).


any(0) ->
    any_value();

any(S) ->
    oneof(any_value_types() ++ [
        ?LAZY(any_list(S)),
        ?LAZY(any_tuple(S))
    ]).


any_value() ->
    oneof(any_value_types()).


any_value_types() ->
    [
        largeint(),
        int(),
        real(),
        atom(),
        binary()
    ].


any_list(0) ->
    [];

any_list(Size) ->
    ListSize = Size div 5,
    vector(ListSize, any(Size div 2)).


any_tuple(0) ->
    {};

any_tuple(Size) ->
    ?LET(L, any_list(Size), list_to_tuple(L)).


% JSON Generation

json() ->
    ?SIZED(Size, json(Size)).


json(0) ->
    oneof([
        json_null(),
        json_true(),
        json_false(),
        json_number(),
        json_string()
    ]);

json(Size) ->
    frequency([
        {1, json_null()},
        {1, json_true()},
        {1, json_false()},
        {1, json_number()},
        {1, json_string()},
        {5, ?LAZY(json_array(Size))},
        {5, ?LAZY(json_object(Size))}
    ]).


json_null() ->
    null.


json_true() ->
    true.


json_false() ->
    false.


json_number() ->
    oneof([largeint(), int(), real()]).


json_string() ->
    utf8().


json_array(0) ->
    [];

json_array(Size) ->
    ArrSize = Size div 5,
    vector(ArrSize, json(Size div 2)).


json_object(0) ->
    {[]};
json_object(Size) ->
    ObjSize = Size div 5,
    {vector(ObjSize, {json_string(), json(Size div 2)})}.


combiner() ->
    ?SIZED(
        Size,
        ?LET(
            L,
            vector((Size div 4) + 1, oneof([$\r, $\n, $\t, $\s])),
            list_to_binary(L)
        )
    ).


atom() ->
    ?LET(L, ?SIZED(Size, vector(Size rem 254, char())), list_to_atom(L)).



%% XXX: Add generators
%
% We should add generators that generate JSON binaries directly
% so we can test things that aren't produced by the encoder.
%
% We should also have a version of the JSON generator that inserts
% errors into the JSON that we can test for.


-endif.
