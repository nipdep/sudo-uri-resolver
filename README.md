# sudo-uri-resolver

FastAPI-based persistent URI resolver for a scholarly KG stored in Apache Jena Fuseki.

## Setup (uv)

```bash
uv sync
```

## Configure (.env)

Create `.env` from `.env.example` and set:

```env
FUSEKI_SERVER_URL=http://spark-6d47:8001/
FUSEKI_DATASET=/idea_kg
PERSISTENT_URI_BASE=https://purl.org/twc/sudo/kg/
PUBLIC_BASE_PATH=/sudo-uri-resolver
```

`PERSISTENT_URI_BASE` defaults to `https://purl.org/twc/sudo/kg/` if omitted.
`PUBLIC_BASE_PATH` defaults to empty (`""`) and should be set when served behind a reverse proxy subpath.
Legacy alias also supported: `RESOLVER_ROOT_PATH`.

## Run

```bash
uv run uvicorn uri_resolver.main:app --reload
```

Base resolver page is available at `/` with:
- centered `SUDO-KG` header and subtitle
- prefix dropdown (`author`, `paper`, `concept`, `artifact`, `proposition`)
- ID search field that normalizes input to lowercase kebab-case before resolving

## Docker

Build image:

```bash
docker build -t sudo-uri-resolver .
```

Run with env vars passed at `docker run`:

```bash
docker run -d -p 8002:8000 \
  -e FUSEKI_SERVER_URL=http://spark-6d47:8001/ \
  -e FUSEKI_DATASET=/idea_kg \
  -e PERSISTENT_URI_BASE=https://purl.org/twc/sudo/kg/ \
  -e PUBLIC_BASE_PATH=/sudo-uri-resolver \
  sudo-uri-resolver
```

Run with a `.env` file:

```bash
docker run --rm -p 8002:8000 --env-file .env sudo-uri-resolver
```

Caddy example for subpath deployment:

```caddyfile
https://spark-6d47.tailb1f37b.ts.net {
  handle_path /sudo-uri-resolver/* {
    reverse_proxy 127.0.0.1:8000 {
      header_up X-Forwarded-Prefix /sudo-uri-resolver
      header_up X-Forwarded-Uri /sudo-uri-resolver{uri}
      header_up X-Forwarded-Proto {scheme}
      header_up X-Forwarded-Host {host}
    }
  }
}
```

The server logs each generated Fuseki query and redirect target at `INFO` level.
Sample log lines:

```text
fuseki_doc_query resource=concept/alice query=SELECT ?p ?o WHERE { <https://purl.org/twc/sudo/kg/concept/alice> ?p ?o } ORDER BY STR(?p) STR(?o) output=application/sparql-results+json target=http://spark-6d47:8001/idea_kg/query?query=...&output=application%2Fsparql-results%2Bjson
doc_rendered resource=concept/alice persistent_uri=https://purl.org/twc/sudo/kg/concept/alice source_url=http://spark-6d47:8001/idea_kg/query?query=...&output=application%2Fsparql-results%2Bjson
fuseki_data_redirect resource=proposition/p1 query=DESCRIBE <https://purl.org/twc/sudo/kg/proposition/p1> output=application/rdf+xml target=http://spark-6d47:8001/idea_kg/query?query=...&output=application%2Frdf%2Bxml
```

## Resolver Flow

- `GET /id/{node_type}/{local_id}`: persistent URI endpoint; negotiates on `Accept` and returns `303` to `/doc/...` or `/data/...{fmt}`.
- `node_type` is constrained to: `author`, `paper`, `concept`, `artifact`, `proposition`.
- `GET /resolve?node_type=...&local_id=...`: helper endpoint used by the homepage search form; normalizes `local_id` to lowercase kebab-case and redirects to `/id/...`.
- `GET /doc/{node_type}/{local_id}`: runs `SELECT ?p ?o WHERE { <PERSISTENT_URI_BASE + node_type + '/' + local_id> ?p ?o }` against Fuseki and renders a formal HTML page (Jinja template + CSS styling).
- If `/doc/{node_type}/{local_id}` returns no triples, resolver redirects to `GET /not-found/{node_type}/{local_id}` (cute HTML 404 page).
- `GET /data/{node_type}/{local_id}`: negotiates machine format and returns `303` to Fuseki `/{dataset}/query`.
- `GET /data/{node_type}/{local_id}.{fmt}`: explicit format endpoint (`jsonld`, `ttl`, `rdf`) returning `303` to Fuseki `/{dataset}/query`.
- For deployments under a prefix like `/sudo-uri-resolver`, set `PUBLIC_BASE_PATH` so `/id` redirects and `/doc` asset/link URLs include the prefix.

Negotiated and data responses include `Vary: Accept`.

## Test

```bash
uv run pytest
```
