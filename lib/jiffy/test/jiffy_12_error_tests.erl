% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_12_error_tests).

-include_lib("eunit/include/eunit.hrl").


enc_invalid_ejson_test_() ->
    Type = invalid_ejson,
    Ref = make_ref(),
    {"invalid_ejson", [
        {"Basic", enc_error(Type, Ref, Ref)},
        {"Nested", enc_error(Type, {Ref, Ref}, {Ref, Ref})}
    ]}.


enc_invalid_string_test_() ->
    Type = invalid_string,
    {"invalid_string", [
        {"Bare strign", enc_error(Type, <<143>>, <<143>>)},
        {"List element", enc_error(Type, <<143>>, [<<143>>])},
        {"Bad obj value", enc_error(Type, <<143>>, {[{foo, <<143>>}]})}
    ]}.

enc_invalid_object_test_() ->
    Type = invalid_object,
    Ref = make_ref(),
    {"invalid_object", [
        {"Number", enc_error(Type, {1}, {1})},
        {"Ref", enc_error(Type, {Ref}, {Ref})},
        {"Tuple", enc_error(Type, {{[]}}, {{[]}})},
        {"Atom", enc_error(Type, {foo}, {foo})}
    ]}.


enc_invalid_object_member_test_() ->
    Type = invalid_object_member,
    {"invalid_object_member", [
        {"Basic", enc_error(Type, foo, {[foo]})},
        {"Basic", enc_error(Type, foo, {[{bar, baz}, foo]})},
        {"Nested", enc_error(Type, foo, {[{bar,{[foo]}}]})},
        {"Nested", enc_error(Type, foo, {[{bar,{[{baz, 1}, foo]}}]})},
        {"In List", enc_error(Type, foo, [{[foo]}])},
        {"In List", enc_error(Type, foo, [{[{bang, true}, foo]}])}
    ]}.


enc_invalid_object_member_arity_test_() ->
    Type = invalid_object_member_arity,
    E1 = {foo},
    E2 = {x, y, z},
    {"invalid_object_member", [
        {"Basic", enc_error(Type, E1, {[E1]})},
        {"Basic", enc_error(Type, E2, {[E2]})},
        {"Basic", enc_error(Type, E1, {[{bar, baz}, E1]})},
        {"Basic", enc_error(Type, E2, {[{bar, baz}, E2]})},
        {"Nested", enc_error(Type, E1, {[{bar,{[E1]}}]})},
        {"Nested", enc_error(Type, E2, {[{bar,{[E2]}}]})},
        {"Nested", enc_error(Type, E1, {[{bar,{[{baz, 1}, E1]}}]})},
        {"Nested", enc_error(Type, E2, {[{bar,{[{baz, 1}, E2]}}]})},
        {"In List", enc_error(Type, E1, [{[E1]}])},
        {"In List", enc_error(Type, E2, [{[E2]}])},
        {"In List", enc_error(Type, E1, [{[{bang, true}, E1]}])},
        {"In List", enc_error(Type, E2, [{[{bang, true}, E2]}])}
    ]}.


enc_invalid_object_member_key_test_() ->
    Type = invalid_object_member_key,
    E1 = {1, true},
    {"invalid_object_member_key", [
        {"Bad string", enc_error(Type, <<143>>, {[{<<143>>, true}]})},
        {"Basic", enc_error(Type, 1, {[{1, true}]})},
        {"Basic", enc_error(Type, [1], {[{[1], true}]})},
        {"Basic", enc_error(Type, {[{foo,bar}]}, {[{{[{foo,bar}]}, true}]})},
        {"Second", enc_error(Type, 1, {[{bar, baz}, E1]})},
        {"Nested", enc_error(Type, 1, {[{bar,{[E1]}}]})},
        {"Nested", enc_error(Type, 1, {[{bar,{[{baz, 1}, E1]}}]})},
        {"In List", enc_error(Type, 1, [{[E1]}])},
        {"In List", enc_error(Type, 1, [{[{bang, true}, E1]}])}
    ]}.



enc_error(Type, Obj, Case) ->
    ?_assertEqual({error, {Type, Obj}}, (catch jiffy:encode(Case))).
