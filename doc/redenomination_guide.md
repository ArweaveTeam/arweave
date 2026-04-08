# Arweave Redenomination Guide for Ecosystem Services

## When Does Redenomination Occur?

Redenomination is triggered when the available supply (total supply + debt
supply - endowment pool) drops below a protocol-defined threshold. This happens
as the endowment pool grows to cover long-term storage costs.

Redenomination is not imminent and may never happen. It is a safety mechanism
to keep the base unit practical if the endowment pool absorbs a large share of
the supply.

When triggered, the redenomination is scheduled 100 blocks (~200 minutes) in
the future. At the scheduled height, each unit becomes 1,000 units in the new
denomination and the block `denomination` field increments by 1.

## Use Explicit Denomination in Transactions

Transactions have a `denomination` field. We recommend always setting it
explicitly rather than leaving it at 0 (implicit/legacy).

- `denomination = 0` — uses the current block's denomination implicitly.
- `denomination >= 1` — explicitly declares the denomination.

To determine the current denomination, read the `denomination` field from the
latest block via `GET /block/current`.

During the transition period around a redenomination event, transactions with
`denomination = 0` are rejected (see Transition Period below). Services that
always set explicit denomination will be unaffected.

## Fee Estimation

Use `/price2/` or `/optimistic_price/` endpoints for fee estimation. They
return the denomination alongside the fee:

- `GET /price2/{bytes}` — estimated fee, no new wallet fee
- `GET /price2/{bytes}/{address}` — estimated fee, includes new wallet fee if the address is not in the account tree
- `GET /optimistic_price/{bytes}` — optimistic (lower) fee estimate
- `GET /optimistic_price/{bytes}/{address}` — optimistic fee with new wallet fee check

Response format:

```json
{
  "fee": "5000000",
  "denomination": 1
}
```

Use the returned `fee` for the transaction `reward` field and the returned
`denomination` for the transaction `denomination` field. Use the same
denomination for the `quantity` field when transferring AR.

The legacy `GET /price/{bytes}` endpoints return a plain integer without
denomination. Migrate to `/price2/` or `/optimistic_price/`.

## Account Balances

- `GET /wallet/{address}/balance` — current balance as a plain integer, in the current denomination
- `GET /block/height/{height}/wallet/{address}/balance` — balance at a specific height, in that block's denomination

These endpoints do not include denomination in the response. Read the
`denomination` field from the corresponding block to interpret the value.

When comparing balances across different block heights, normalize using each
block's `denomination` field.

## Transition Period

When redenomination is scheduled, there is a transition window of up to 100
blocks (~200 minutes) during which transactions with `denomination = 0` are
rejected. This prevents ambiguity about which denomination a transaction
intends.

- Once `redenomination_height` is set in block headers, transactions with
  `denomination = 0` are rejected until the redenomination height is reached.
- Transactions with explicit denomination (`1 <= denomination <= current block
  denomination`) remain valid throughout.

Services that always set explicit denomination experience no disruption.

## Monitoring

Monitor the `redenomination_height` field in block headers via
`GET /block/current`:

- `redenomination_height = 0` — no redenomination scheduled.
- `redenomination_height > 0` — redenomination scheduled at that height;
  ~100 blocks to prepare.
