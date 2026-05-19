**This is an alpha update and may not be ready for production use. This software was prepared by the Digital History Association, in cooperation from the wider Arweave ecosystem.**


## Data Sync

### The problem

Set of candidate peers periodically drops low or to zero while sycning

### The fix

Optimize peer discovery and sync_task queue:

One process per peer rather than one process per storage module. This prevents a lot of rate limiting that used to happen before when multiple storage module processes would hit the same peer concurrently. Now there's one process that can manage backpressure when making GET /data_sync_record to a peer

Several bug fixes on peer expiration, and "leak" bugs preventing data sync being completed.

## Rate-limiting 

### Scalability

Server-side rate limiter has been improved to handle requests better under higher loads. Now, multiple rate-limiter processes handle the requests per rate-limiting-groups. 

Rate-limiting timeout has been lowered to 1000ms, if this is breached requests will be responded with status 503.

### Quota headers

Additional headers are present in the responses of the HTTP API. These headers provide information on the current quota for the calling endpoint, and will be utilised to optimise the client throttling in future releases.

Please see [this link](https://www.ietf.org/archive/id/draft-polli-ratelimit-headers-02.html) for documentation and further information on the headers.

## Further improvements

* Optimised data table use for ar_data_discovery, as it previously could get bogged down removing empty peers.
* Several logging optimisations have been made to reduce repetive messages.
