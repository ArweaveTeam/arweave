# Querying node metrics

Use a node's own HTTP API for live or point-in-time metrics. Use the Prometheus
datasource through Grafana for historical data, rates, trends, or incident
correlation, and whenever the node is not directly reachable from the agent.

## Live metrics

Fetch live metrics from `http://<host>:<port>/metrics`. Use `localhost` only
when the agent runs on the node itself; otherwise use the node's reachable
address. The HTTP API port defaults to `1984` but is configurable, so check the
node configuration or launch arguments rather than assuming the default.

## One-time Grafana setup

Create a read-only Grafana service-account token with the Viewer role. Store it,
the Grafana base URL, and the Prometheus datasource UID in a mode-600 env file
outside the repository. Do this in your own shell, not through an agent command,
so the token never enters an agent transcript.

```bash
umask 077 && mkdir -p "$HOME/.config"
cat > "$HOME/.config/grafana_token.env" <<'EOF'
export GRAFANA_METRICS_TOKEN=<read-only service-account token>
export GRAFANA_URL=https://<your-org>.grafana.net
export GRAFANA_DATASOURCE_UID=<prometheus datasource uid>
EOF
chmod 600 "$HOME/.config/grafana_token.env"
```

For self-hosted Grafana, use its base URL, such as `http://<host>:3000`. Find
the datasource UID under Grafana -> Connections -> Data sources -> Prometheus;
it is the `uid` in the page URL. You can also list datasources:

```bash
curl -sH "Authorization: Bearer $GRAFANA_METRICS_TOKEN" \
    "$GRAFANA_URL/api/datasources"
```

## Querying through Grafana

Source the env file at the start of each query and reference the variables.
Never hard-code, print, or dump the token, and never pass `curl -v`. Writing
`"$GRAFANA_METRICS_TOKEN"` in a command is safe because the shell expands it at
runtime, so only the variable name is logged.

If `$HOME/.config/grafana_token.env` is absent, fall back to the node's own
`/metrics` endpoint for live data and tell the user that historical queries
require the env file setup above.

Instant query:

```bash
source "$HOME/.config/grafana_token.env"
curl -sG -H "Authorization: Bearer $GRAFANA_METRICS_TOKEN" \
    "$GRAFANA_URL/api/datasources/proxy/uid/$GRAFANA_DATASOURCE_UID/api/v1/query" \
    --data-urlencode 'query=sum(chunks_stored)'
```

Range query:

```bash
source "$HOME/.config/grafana_token.env"
curl -sG -H "Authorization: Bearer $GRAFANA_METRICS_TOKEN" \
    "$GRAFANA_URL/api/datasources/proxy/uid/$GRAFANA_DATASOURCE_UID/api/v1/query_range" \
    --data-urlencode 'query=rate(chunks_stored[1m])' \
    --data-urlencode "start=$(date -d '4 hours ago' +%s)" \
    --data-urlencode "end=$(date +%s)" \
    --data-urlencode 'step=30'
```

One Grafana usually scrapes many nodes. Filter the node with an `instance`
label selector such as `chunks_stored{instance=~"<node>.*"}`. List the known
instance label values through `.../api/v1/label/instance/values`.

Useful sync metrics include:

- `chunks_stored`
- `chunk_cache_size`
- `sync_claimed_bytes_by_store`
- `sync_tasks_by_store`
- `chunk_interval_cache_size`
- `http_client_get_chunk_duration_seconds_sum`
- `http_client_get_chunk_duration_seconds_count`
- `sync_discovery_peers`
