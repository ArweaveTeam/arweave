%%% State shared with focused tests.

-record(rate, {
    balance = infinity,
    refill_ms,
    wakeup_scheduled = false
}).
