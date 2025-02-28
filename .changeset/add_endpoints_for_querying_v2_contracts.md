---
default: minor
---

# Add endpoints for querying V2 contracts

Adds two new endpoints for querying V2 contracts

### `[GET] /v2/contracts/:id`

Returns a single V2 contract by its ID

### `[POST] /v2/contracts`

Queries a list of contracts with optional filtering

**Example request  body**
```json
{
  "statuses": [],
  "contractIDs": [],
  "renewedFrom": [],
  "renewedTo": [],
  "renterKey": [],
  "minNegotiationHeight": 0,
  "maxNegotiationHeight": 0,
  "minExpirationHeight": 0,
  "maxExpirationHeight": 0,
  "limit": 0,
  "offset": 0,
  "sortField": "",
  "sortDesc": false
}
```

