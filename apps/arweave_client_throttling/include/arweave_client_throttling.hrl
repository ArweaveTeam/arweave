-ifndef(ARWEAVE_CLIENT_THROTTLING_HRL).
-define(ARWEAVE_CLIENT_THROTTLING_HRL, true).

%% Fraction of the configured `initial_remaining' that the new
%% `remaining' reported by a remote response must differ by, relative
%% to the value already stored for the peer, before we bother to
%% update the peer state. Updates reporting `remaining = 0' are always
%% applied regardless of this ratio.
-define(SIGNIFICANTLY_DIFFERENT_RATIO, 0.1).

-endif.
