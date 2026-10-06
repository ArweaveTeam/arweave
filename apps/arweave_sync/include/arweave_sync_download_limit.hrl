-record(rate, {
    %% Each request reduces this balance. When the balance is 0, no requests
    %% are allowed until it is refilled.
    balance = infinity,
    %% Monotonic time of the last refill, in milliseconds.
    refill_ms,
    wakeup_scheduled = false
}).
