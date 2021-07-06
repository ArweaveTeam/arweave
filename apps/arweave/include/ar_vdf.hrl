% 25 checkpoints 40 ms each = 1000 ms
-define(VDF_STEP_COUNT_IN_CHECKPOINT, 25).

-define(VDF_BYTE_SIZE, 32).

%% Typical ryzen 5900X iterations for 1 sec
-define(VDF_SHA_1S, 15000000).

-ifdef(DEBUG).
-define(VDF_DIFFICULTY, 2).
-else.
-define(VDF_DIFFICULTY, ?VDF_SHA_1S div ?VDF_STEP_COUNT_IN_CHECKPOINT).
-endif.
