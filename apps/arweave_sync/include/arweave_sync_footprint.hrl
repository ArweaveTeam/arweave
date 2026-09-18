%%% State shared with focused tests.

%% Mutable footprint snapshot for one scheduler dispatch pass. Deferred
%% reservations and the draining-footprint count are reset each pass.
-record(dispatch, {
    reservations = #{},
    max_active,
    deferred = sets:new(),
    footprints_draining = 0
}).
