% 25 checkpoints 40 ms each = 1000 ms
-define(VDF_CHECKPOINT_COUNT_IN_STEP, 25).

-define(VDF_BYTE_SIZE, 32).

%% Typical ryzen 5900X iterations for 1 sec
-define(VDF_SHA_1S, 15_000_000).

-ifdef(DEBUG).
-define(VDF_DIFFICULTY, 2).
-define(VDF_DIFFICULTY_RETARGET, 20).
-define(VDF_HISTORY_CUT, 2).
-else.
-define(VDF_DIFFICULTY, ?VDF_SHA_1S div ?VDF_CHECKPOINT_COUNT_IN_STEP).
	-ifndef(VDF_DIFFICULTY_RETARGET).
		-define(VDF_DIFFICULTY_RETARGET, 720).
	-endif.
	-ifndef(VDF_HISTORY_CUT).
		-define(VDF_HISTORY_CUT, 50).
	-endif.
-endif.
