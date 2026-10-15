-ifndef(ARWEAVE_LIB_CONSTANTS_HRL).
-define(ARWEAVE_LIB_CONSTANTS_HRL, true).

%% Protocol constants. Keep build-time overrides in these definitions; do not
%% copy their values into consumers. Runtime geometry and fork heights use
%% arweave_lib_constants.
-include("arweave_lib_constants_base.hrl").
-include("arweave_lib_constants_consensus.hrl").
-include("arweave_lib_constants_vdf.hrl").
-include("arweave_lib_constants_pricing.hrl").
-include("arweave_lib_constants_inflation.hrl").

%% The number of block intervals retained for VDF difficulty adjustment.
-ifdef(AR_TEST).
-define(BLOCK_TIME_HISTORY_BLOCKS, 3).
-else.
-ifndef(BLOCK_TIME_HISTORY_BLOCKS).
-define(BLOCK_TIME_HISTORY_BLOCKS, (30 * 24 * 30)).
-endif.
-endif.

%% The unconditional difficulty reduction coefficient applied at the
%% first 2.5 block.
-define(DIFF_DROP_2_5, 2).

%% The unconditional difficulty reduction coefficient applied at the
%% first 2.6 block.
-define(INITIAL_DIFF_DROP_2_6, 100).

%% The additional difficulty reduction coefficient applied every 10 minutes at the
%% first 2.6 block.
-define(DIFF_DROP_2_6, 2).

%% The unconditional difficulty reduction coefficient applied at the
%% first 2.7.2 block.
-define(INITIAL_DIFF_DROP_2_7_2, 10).

%% The additional difficulty reduction coefficient applied every 10 minutes at the
%% first 2.7.2 block.
-define(DIFF_DROP_2_7_2, 2).

-endif.
