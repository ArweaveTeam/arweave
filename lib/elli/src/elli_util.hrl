-define(I2L(I), integer_to_list(I)).

-ifdef('16.0').
-define(B2I(I), binary_to_integer(I)).
-else.
-define(B2I(I), list_to_integer(binary_to_list(I))).
-endif.

-ifdef('21.0').
-include_lib("kernel/include/logger.hrl").
-else.
-define(LOG_ERROR(Str), error_logger:error_msg(Str)).
-define(LOG_ERROR(Format,Data), error_logger:error_msg(Format, Data)).
-define(LOG_INFO(Format,Data), error_logger:info_msg(Format, Data)).
-endif.

-ifdef('21.0').
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(),).
-endif.

%% Bloody useful
-define(IF(Test,True,False), case Test of true -> True; false -> False end).
