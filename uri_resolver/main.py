from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from collections import OrderedDict
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request as URLRequest
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from rdflib import Dataset, Graph, URIRef
from rdflib.namespace import RDF
from rdflib.term import BNode, Literal

from .backend import FusekiRedirectBackend, NQUADS, SPARQL_JSON, TURTLE
from .models import NodeType, ResourceIdentifier
from .services import (
    MEDIA_TYPE_TO_FORMAT,
    NotAcceptableError,
    ResolverService,
    UnsupportedFormatError,
)
from .settings import AppSettings

logger = logging.getLogger("uri_resolver.doc")
request_logger = logging.getLogger("uri_resolver.request")

PREFIX_MAP = (
    ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"),
    ("http://www.w3.org/2000/01/rdf-schema#", "rdfs:"),
    ("http://www.w3.org/2002/07/owl#", "owl:"),
    ("http://www.w3.org/2004/02/skos/core#", "skos:"),
    ("http://www.w3.org/ns/prov#", "prov:"),
    ("http://schema.org/", "schema:"),
    ("http://purl.org/dc/terms/", "dcterms:"),
    ("https://w3id.org/twc/sudo/ontology#", "sudo:"),
    ("http://purl.org/spar/doco/", "doco:"),
    ("http://purl.org/spar/deo/", "deo:"),
    ("http://www.wikidata.org/prop/direct/", "wdt:"),
)

RDF_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS_PREFIX = "http://www.w3.org/2000/01/rdf-schema#"
SKOS_PREFIX = "http://www.w3.org/2004/02/skos/core#"
SCHEMA_PREFIX = "http://schema.org/"
DCTERMS_PREFIX = "http://purl.org/dc/terms/"
OWL_PREFIX = "http://www.w3.org/2002/07/owl#"
SUDO_PREFIX = "https://w3id.org/twc/sudo/ontology#"
PROV_PREFIX = "http://www.w3.org/ns/prov#"

TITLE_PREDICATES = {
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://schema.org/name",
    "http://purl.org/dc/terms/title",
}

DESCRIPTION_PREDICATES = {
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://schema.org/description",
    "http://purl.org/dc/terms/description",
}

LABEL_PREDICATES = (
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://xmlns.com/foaf/0.1/name",
    "http://schema.org/name",
    "http://purl.org/dc/terms/title",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value",
)

PROV_PRIMARY_SOURCE = "http://www.w3.org/ns/prov#hadPrimarySource"
PROV_WAS_DERIVED_FROM = "http://www.w3.org/ns/prov#wasDerivedFrom"
PATTERN_CONTAINS = "http://www.essepuntato.it/2008/12/pattern#contains"
PATTERN_CONTAINS_HEADER = "http://www.essepuntato.it/2008/12/pattern#containsAsHeader"
ARGUMENTATION_PREFIX = "https://w3id.org/twc/sudo/ontology#"
POSITION_PREDICATE = "https://w3id.org/twc/sudo/kg/position"
EX_POSITION_PREDICATE = "https://w3id.org/twc/sudo/kg/position"

DESCRIBE_METADATA_LABELS = {
    "http://purl.org/dc/terms/title": "Title",
    "http://purl.org/dc/terms/creator": "Authors",
    "http://purl.org/dc/terms/issued": "Published",
    "http://purl.org/dc/terms/isPartOf": "Venue",
    "http://purl.org/dc/terms/subject": "Subjects",
    "http://purl.org/ontology/bibo/doi": "DOI",
    "https://w3id.org/twc/sudo/kg/citationCount": "Citation Count",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso": "See Also",
    "http://schema.org/description": "Abstract",
    "http://purl.org/dc/terms/description": "Abstract",
}

NODE_TYPE_OPTIONS = (
    ("Author", NodeType.author),
    ("Paper", NodeType.paper),
    ("Concept", NodeType.concept),
    ("Proposition", NodeType.proposition),
)

def _join_public_base_path(public_base_path: str, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    if not public_base_path:
        return normalized_path
    return f"{public_base_path}{normalized_path}"


def _static_path_for(public_base_path: str, asset_path: str) -> str:
    return _join_public_base_path(public_base_path, f"/static/{asset_path.lstrip('/')}")


def _normalize_public_base_path(value: str | None) -> str:
    if value is None:
        return ""

    normalized = value.strip()
    if normalized in {"", "/"}:
        return ""
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized.rstrip("/")


def _infer_public_base_path_from_uri_header(request: Request, header_name: str) -> str:
    raw_value = request.headers.get(header_name, "")
    header_uri = raw_value.split(",", 1)[0].strip()
    if not header_uri:
        return ""

    original_path = header_uri.split("?", 1)[0]
    current_path = request.url.path
    if not current_path:
        return ""

    if original_path == current_path:
        return ""
    if not original_path.endswith(current_path):
        return ""

    prefix = original_path[: -len(current_path)]
    return _normalize_public_base_path(prefix)


def _normalize_local_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


class _StripPublicBasePathMiddleware:
    def __init__(self, app, public_base_path: str) -> None:
        self.app = app
        self.public_base_path = public_base_path.rstrip("/")

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or not self.public_base_path:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        prefix = self.public_base_path
        if path == prefix:
            scope = dict(scope)
            scope["path"] = "/"
            await self.app(scope, receive, send)
            return

        if path.startswith(f"{prefix}/"):
            scope = dict(scope)
            scope["path"] = path[len(prefix) :]
            await self.app(scope, receive, send)
            return

        await self.app(scope, receive, send)


def _request_header_snapshot(request: Request) -> dict[str, str]:
    interesting_headers = (
        "host",
        "accept",
        "user-agent",
        "referer",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-forwarded-prefix",
        "x-forwarded-uri",
        "x-original-uri",
    )
    snapshot: dict[str, str] = {}
    for header_name in interesting_headers:
        header_value = request.headers.get(header_name)
        if header_value:
            snapshot[header_name] = header_value
    return snapshot


def fetch_doc_graph(
    url: str,
    timeout: float = 10.0,
) -> tuple[Graph, str]:
    """Fetch RDF from Fuseki for HTML rendering."""
    request = URLRequest(
        url=url,
        headers={
            "Accept": TURTLE,
            "User-Agent": "sudo-uri-resolver/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read()
            content_type = response.headers.get("Content-Type", TURTLE)
            charset = response.headers.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            graph = Graph()
            graph.parse(data=text, format=_rdflib_format(content_type))
            return graph, content_type
    except HTTPError as exc:
        message = f"Fuseki returned HTTP {exc.code}"
        if exc.fp:
            detail = exc.read().decode("utf-8", errors="replace")
            message = f"{message}: {detail[:300]}"
        raise RuntimeError(message) from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Fuseki: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Fuseki response could not be parsed as RDF: {exc}") from exc


def fetch_sparql_bindings(
    url: str,
    timeout: float = 10.0,
) -> list[dict[str, object]]:
    request = URLRequest(
        url=url,
        headers={
            "Accept": SPARQL_JSON,
            "User-Agent": "sudo-uri-resolver/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            body = json.loads(text)
            results = body.get("results", {})
            bindings = results.get("bindings", [])
            return bindings if isinstance(bindings, list) else []
    except HTTPError as exc:
        message = f"Fuseki returned HTTP {exc.code}"
        if exc.fp:
            detail = exc.read().decode("utf-8", errors="replace")
            message = f"{message}: {detail[:300]}"
        raise RuntimeError(message) from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Fuseki: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Fuseki response could not be parsed as SPARQL JSON: {exc}") from exc


def fetch_quad_dataset(
    url: str,
    timeout: float = 10.0,
) -> tuple[Dataset, str]:
    request = URLRequest(
        url=url,
        headers={
            "Accept": NQUADS,
            "User-Agent": "sudo-uri-resolver/0.1",
        },
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            payload = response.read()
            content_type = response.headers.get("Content-Type", NQUADS)
            charset = response.headers.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            dataset = Dataset()
            dataset.parse(data=text, format="nquads")
            return dataset, content_type
    except HTTPError as exc:
        message = f"Fuseki returned HTTP {exc.code}"
        if exc.fp:
            detail = exc.read().decode("utf-8", errors="replace")
            message = f"{message}: {detail[:300]}"
        raise RuntimeError(message) from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Fuseki: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Fuseki response could not be parsed as N-Quads: {exc}") from exc


def _rdflib_format(content_type: str) -> str:
    normalized = content_type.split(";", 1)[0].strip().lower()
    if normalized == "text/turtle":
        return "turtle"
    if normalized == "application/ld+json":
        return "json-ld"
    if normalized == "application/rdf+xml":
        return "xml"
    if normalized == "application/n-triples":
        return "nt"
    return "turtle"


def fetch_fuseki_dataset_names(
    server_url: str,
    timeout: float = 10.0,
) -> list[str]:
    """Fetch dataset names exposed by a Fuseki server's admin API."""
    admin_urls = (
        f"{server_url.rstrip('/')}/$/datasets",
        f"{server_url.rstrip('/')}/$/server",
    )

    last_error: Exception | None = None
    for admin_url in admin_urls:
        request = URLRequest(
            url=admin_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "sudo-uri-resolver/0.1",
            },
        )

        try:
            with urlopen(request, timeout=timeout) as response:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                text = payload.decode(charset, errors="replace")
                datasets = _extract_dataset_names(json.loads(text))
                if datasets:
                    return datasets
        except HTTPError as exc:
            last_error = exc
            continue
        except URLError as exc:
            last_error = exc
            continue
        except json.JSONDecodeError as exc:
            last_error = exc
            continue

    if last_error is not None:
        logger.info("fuseki_dataset_discovery_failed error=%s", last_error)

    return []


def _extract_dataset_names(payload: object) -> list[str]:
    raw_datasets: object = payload
    if isinstance(payload, dict):
        for key in ("datasets", "dataset", "services"):
            if key in payload:
                raw_datasets = payload[key]
                break

    candidate_names: list[str] = []

    if isinstance(raw_datasets, dict):
        for key, value in raw_datasets.items():
            if isinstance(key, str) and key.strip():
                candidate_names.append(key)
            if isinstance(value, dict):
                for field in ("name", "dbName", "dataset", "id"):
                    raw_name = value.get(field)
                    if isinstance(raw_name, str) and raw_name.strip():
                        candidate_names.append(raw_name)
                        break
    elif isinstance(raw_datasets, list):
        for item in raw_datasets:
            if isinstance(item, str) and item.strip():
                candidate_names.append(item)
                continue
            if isinstance(item, dict):
                for field in ("name", "dbName", "dataset", "id"):
                    raw_name = item.get(field)
                    if isinstance(raw_name, str) and raw_name.strip():
                        candidate_names.append(raw_name)
                        break

    normalized_names: list[str] = []
    seen: set[str] = set()
    for name in candidate_names:
        normalized = name.strip().strip("/")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_names.append(normalized)

    return normalized_names


def _ordered_dataset_names(default_dataset: str, discovered_datasets: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    for dataset_name in [default_dataset, *discovered_datasets]:
        normalized = dataset_name.strip().strip("/")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)

    return ordered


def _resource_path(resource_type: str, local_id: str) -> str:
    return f"{resource_type.strip('/')}/{local_id.strip('/')}"


def _resource_parts(resource_path: str) -> tuple[str, str]:
    normalized = resource_path.strip("/")
    if "/" not in normalized:
        return normalized, normalized
    resource_type, local_id = normalized.split("/", 1)
    return resource_type, local_id


def _compact_uri(uri: str) -> str:
    for base, prefix in PREFIX_MAP:
        if uri.startswith(base):
            return f"{prefix}{uri[len(base):]}"

    for separator in ("#", "/"):
        if separator in uri:
            tail = uri.rsplit(separator, 1)[-1]
            if tail:
                return tail
    return uri


def _normalize_ws(value: str) -> str:
    return " ".join(value.split())


def _local_name(uri: str) -> str:
    compact = _compact_uri(uri)
    if ":" in compact:
        compact = compact.split(":", 1)[1]
    return compact


def _humanize_identifier(value: str) -> str:
    return _normalize_ws(value.replace("_", " ").replace("-", " "))


def _sort_key_for_uri(uri: URIRef, graph: Graph) -> tuple[int, str]:
    position_predicates = (
        URIRef("https://w3id.org/twc/sudo/kg/position"),
        URIRef("https://w3id.org/twc/sudo/kg/ex:position"),
        URIRef("https://w3id.org/twc/sudo/kg/position"),
        URIRef("https://w3id.org/twc/sudo/kg/ex_position"),
    )
    for predicate in position_predicates:
        for obj in graph.objects(uri, predicate):
            try:
                return int(str(obj)), str(uri)
            except ValueError:
                continue
    return 10**9, str(uri)


def _ordered_uri_objects(graph: Graph, subject: URIRef, predicate_uri: str) -> list[URIRef]:
    values = [obj for obj in graph.objects(subject, URIRef(predicate_uri)) if isinstance(obj, URIRef)]
    return sorted(values, key=lambda uri: _sort_key_for_uri(uri, graph))


def _build_label_map(graph: Graph) -> dict[URIRef, str]:
    label_map: dict[URIRef, str] = {}
    for predicate_uri in LABEL_PREDICATES:
        predicate = URIRef(predicate_uri)
        for subject, obj in graph.subject_objects(predicate):
            if subject in label_map or not isinstance(obj, Literal):
                continue
            label = _normalize_ws(str(obj))
            if label:
                label_map[subject] = label
    return label_map


def _resolve_node_label(node: URIRef, label_map: dict[URIRef, str]) -> str:
    label = label_map.get(node, "")
    if label:
        return label
    return _humanize_identifier(_local_name(str(node)))


def _node_type_labels(graph: Graph, node: URIRef) -> list[str]:
    return sorted(
        {
            _compact_uri(str(obj))
            for obj in graph.objects(node, RDF.type)
            if isinstance(obj, URIRef)
        }
    )


def _classify_describe_kind(type_labels: list[str]) -> str:
    lowered = {label.lower() for label in type_labels}
    if any("sudo:artifact" == label for label in lowered):
        return "artifact"
    if any("sudo:argument" == label for label in lowered):
        return "argument"
    if any("sudo:descriptor" == label for label in lowered):
        return "descriptor"
    if any(label.endswith("researchpaper") or label.endswith(":paper") for label in lowered):
        return "paper"
    return "generic"


def _is_topic_node(type_labels: list[str]) -> bool:
    lowered = {label.lower() for label in type_labels}
    return any(label == "topic" or label.endswith(":topic") for label in lowered)


def _describe_resource_label(uri: str, persistent_uri_base: str) -> str:
    normalized_base = persistent_uri_base.rstrip("/")
    if uri.startswith(normalized_base):
        return uri[len(normalized_base) :].lstrip("/")
    return uri


def _merge_graphs(graphs: list[Graph]) -> Graph:
    merged = Graph()
    for graph in graphs:
        for triple in graph:
            merged.add(triple)
    return merged


def _serialize_graph(graph: Graph, fmt: str = "turtle") -> str:
    serialized = graph.serialize(format=fmt)
    if isinstance(serialized, bytes):
        return serialized.decode("utf-8", errors="replace")
    return str(serialized)


def _fetch_select_bindings_from_datasets(
    backend: FusekiRedirectBackend,
    resource_label: str,
    query: str,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
) -> list[dict[str, object]]:
    collected: list[dict[str, object]] = []
    seen: set[str] = set()
    for dataset_name in dataset_names:
        source_url = backend.get_select_target(
            resource_label=resource_label,
            query=query,
            dataset=dataset_name,
        )
        _increment_query_counter(query_counter)
        try:
            bindings = fetch_sparql_bindings(source_url)
        except RuntimeError:
            continue
        for binding in bindings:
            key = json.dumps(binding, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            collected.append(binding)
    return collected


def _fetch_quad_dataset_from_datasets(
    backend: FusekiRedirectBackend,
    resource_label: str,
    query: str,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
) -> Dataset:
    merged = Dataset()
    for dataset_name in dataset_names:
        source_url = backend.get_nquads_target(
            resource_label=resource_label,
            query=query,
            dataset=dataset_name,
        )
        _increment_query_counter(query_counter)
        try:
            dataset, _content_type = fetch_quad_dataset(source_url)
        except RuntimeError:
            continue
        for quad in dataset.quads((None, None, None, None)):
            merged.add(quad)
    return merged


def _incident_quads_query(target_uri: URIRef) -> str:
    return (
        "CONSTRUCT { "
        "GRAPH ?g { <"
        f"{target_uri}"
        "> ?p ?o . } "
        "GRAPH ?g { ?s ?pin <"
        f"{target_uri}"
        "> . } "
        "} WHERE { "
        "{ GRAPH ?g { <"
        f"{target_uri}"
        "> ?p ?o . } } "
        "UNION "
        "{ GRAPH ?g { ?s ?pin <"
        f"{target_uri}"
        "> . } } "
        "UNION "
        "{ <"
        f"{target_uri}"
        "> ?p ?o . BIND(<urn:sudo:graph/default> AS ?g) } "
        "UNION "
        "{ ?s ?pin <"
        f"{target_uri}"
        "> . BIND(<urn:sudo:graph/default> AS ?g) } "
        "}"
    )


def _neighborhood_query(target_uri: URIRef) -> str:
    return (
        "SELECT DISTINCT ?node ?predicate ?direction ?graph WHERE { "
        "{ GRAPH ?graph { <"
        f"{target_uri}"
        "> ?predicate ?node . FILTER(isIRI(?node)) } BIND(\"outgoing\" AS ?direction) } "
        "UNION "
        "{ GRAPH ?graph { ?node ?predicate <"
        f"{target_uri}"
        "> . FILTER(isIRI(?node)) } BIND(\"incoming\" AS ?direction) } "
        "UNION "
        "{ <"
        f"{target_uri}"
        "> ?predicate ?node . FILTER(isIRI(?node)) BIND(\"outgoing\" AS ?direction) BIND(\"default\" AS ?graph) } "
        "UNION "
        "{ ?node ?predicate <"
        f"{target_uri}"
        "> . FILTER(isIRI(?node)) BIND(\"incoming\" AS ?direction) BIND(\"default\" AS ?graph) } "
        "}"
    )


def _node_neighborhood_map(
    backend: FusekiRedirectBackend,
    dataset_names: list[str],
    target_uri: URIRef,
    query_counter: dict[str, int] | None = None,
) -> dict[URIRef, dict[str, set[str]]]:
    bindings = _fetch_select_bindings_from_datasets(
        backend=backend,
        resource_label=_compact_uri(str(target_uri)),
        query=_neighborhood_query(target_uri),
        dataset_names=dataset_names,
        query_counter=query_counter,
    )
    neighborhood: dict[URIRef, dict[str, set[str]]] = {}
    for binding in bindings:
        node_value = binding.get("node", {})
        predicate_value = binding.get("predicate", {})
        direction_value = binding.get("direction", {})
        if not isinstance(node_value, dict) or not isinstance(predicate_value, dict) or not isinstance(direction_value, dict):
            continue
        node_uri = node_value.get("value")
        predicate_uri = predicate_value.get("value")
        direction = direction_value.get("value")
        if not isinstance(node_uri, str) or not isinstance(predicate_uri, str) or direction not in {"incoming", "outgoing"}:
            continue
        neighborhood.setdefault(
            URIRef(node_uri),
            {
                "incoming": set(),
                "outgoing": set(),
            },
        )[direction].add(predicate_uri)
    return neighborhood


def _inverse_uri_map(
    backend: FusekiRedirectBackend,
    dataset_names: list[str],
    object_uri: URIRef,
    query_counter: dict[str, int] | None = None,
) -> dict[URIRef, set[str]]:
    inverse_map: dict[URIRef, set[str]] = {}
    for node_uri, directions in _node_neighborhood_map(
        backend,
        dataset_names,
        object_uri,
        query_counter=query_counter,
    ).items():
        for predicate_uri in directions["incoming"]:
            inverse_map.setdefault(node_uri, set()).add(predicate_uri)
    return inverse_map


def _fetch_uri_graphs(
    backend: FusekiRedirectBackend,
    persistent_uri_base: str,
    nodes: set[URIRef],
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
) -> list[Graph]:
    related_graphs: list[Graph] = []
    for node in sorted(nodes, key=str):
        resolved = _fetch_doc_graph_from_datasets(
            backend=backend,
            resource_label=_describe_resource_label(str(node), persistent_uri_base),
            persistent_uri=str(node),
            dataset_names=dataset_names,
            query_counter=query_counter,
        )
        if resolved is None:
            continue
        related_graphs.append(resolved[0])
    return related_graphs


def _artifact_related_graphs(
    backend: FusekiRedirectBackend,
    persistent_uri_base: str,
    graph: Graph,
    root_uri: URIRef,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
    neighborhood: dict[URIRef, dict[str, set[str]]] | None = None,
) -> list[Graph]:
    neighborhood = neighborhood or _node_neighborhood_map(
        backend,
        dataset_names,
        root_uri,
        query_counter=query_counter,
    )
    neighborhood_nodes = set(neighborhood.keys())
    proposition_nodes = {
        node
        for node, directions in neighborhood.items()
        if directions["incoming"] or directions["outgoing"]
    }
    neighborhood_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=neighborhood_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )
    proposition_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=proposition_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )

    proposition_graph = _merge_graphs([*neighborhood_graphs, *proposition_graphs])
    sentence_nodes: set[URIRef] = set()
    paper_nodes: set[URIRef] = set()
    for node in proposition_nodes | {root_uri}:
        sentence_nodes.update(_ordered_uri_objects(proposition_graph, node, PROV_WAS_DERIVED_FROM))
        paper_nodes.update(_ordered_uri_objects(proposition_graph, node, PROV_PRIMARY_SOURCE))
        sentence_nodes.update(_ordered_uri_objects(graph, node, PROV_WAS_DERIVED_FROM))
        paper_nodes.update(_ordered_uri_objects(graph, node, PROV_PRIMARY_SOURCE))

    sentence_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=sentence_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )
    paper_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=paper_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )

    paragraph_nodes: set[URIRef] = set()
    for sentence in sentence_nodes:
        paragraph_nodes.update(_inverse_uri_map(backend, dataset_names, sentence, query_counter=query_counter).keys())
    paragraph_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=paragraph_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )

    section_nodes: set[URIRef] = set()
    for paragraph in paragraph_nodes:
        section_nodes.update(_inverse_uri_map(backend, dataset_names, paragraph, query_counter=query_counter).keys())
    section_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=section_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )

    section_graph = _merge_graphs(section_graphs)
    header_nodes: set[URIRef] = set()
    for section in section_nodes:
        header_nodes.update(_ordered_uri_objects(section_graph, section, PATTERN_CONTAINS_HEADER))
    header_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=header_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )

    return [
        *neighborhood_graphs,
        *proposition_graphs,
        *sentence_graphs,
        *paper_graphs,
        *paragraph_graphs,
        *section_graphs,
        *header_graphs,
    ]


def _topic_related_graphs(
    backend: FusekiRedirectBackend,
    persistent_uri_base: str,
    graph: Graph,
    root_uri: URIRef,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
    neighborhood: dict[URIRef, dict[str, set[str]]] | None = None,
) -> list[Graph]:
    neighborhood = neighborhood or _node_neighborhood_map(
        backend,
        dataset_names,
        root_uri,
        query_counter=query_counter,
    )
    neighborhood_nodes = set(neighborhood.keys())
    neighborhood_graphs = _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=neighborhood_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )
    merged_neighborhood = _merge_graphs([graph, *neighborhood_graphs])
    artifact_nodes = {
        node
        for node in neighborhood_nodes
        if _classify_describe_kind(_node_type_labels(merged_neighborhood, node)) == "artifact"
    }

    artifact_related_graphs: list[Graph] = []
    for artifact_node in sorted(artifact_nodes, key=str):
        artifact_related_graphs.extend(
            _artifact_related_graphs(
                backend=backend,
                persistent_uri_base=persistent_uri_base,
                graph=merged_neighborhood,
                root_uri=artifact_node,
                dataset_names=dataset_names,
                query_counter=query_counter,
                neighborhood=None,
            )
        )

    return [*neighborhood_graphs, *artifact_related_graphs]


def _fetch_related_graphs(
    backend: FusekiRedirectBackend,
    persistent_uri_base: str,
    graph: Graph,
    root_uri: URIRef,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
    neighborhood: dict[URIRef, dict[str, set[str]]] | None = None,
) -> list[Graph]:
    type_labels = _node_type_labels(graph, root_uri)
    describe_kind = _classify_describe_kind(type_labels)
    if describe_kind == "artifact":
        return _artifact_related_graphs(
            backend=backend,
            persistent_uri_base=persistent_uri_base,
            graph=graph,
            root_uri=root_uri,
            dataset_names=dataset_names,
            query_counter=query_counter,
            neighborhood=neighborhood,
        )
    if _is_topic_node(type_labels):
        return _topic_related_graphs(
            backend=backend,
            persistent_uri_base=persistent_uri_base,
            graph=graph,
            root_uri=root_uri,
            dataset_names=dataset_names,
            query_counter=query_counter,
            neighborhood=neighborhood,
        )

    neighborhood_map = neighborhood or _node_neighborhood_map(
        backend,
        dataset_names,
        root_uri,
        query_counter=query_counter,
    )
    neighborhood_nodes = set(neighborhood_map.keys())
    neighborhood_nodes.update(
        obj
        for _, obj in graph.predicate_objects(root_uri)
        if isinstance(obj, URIRef)
    )
    return _fetch_uri_graphs(
        backend=backend,
        persistent_uri_base=persistent_uri_base,
        nodes=neighborhood_nodes,
        dataset_names=dataset_names,
        query_counter=query_counter,
    )


def _statement_items_for_subject(
    graph: Graph,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    statements_by_predicate: dict[str, dict[str, object]] = {}
    for predicate, obj in graph.predicate_objects(subject):
        predicate_uri = str(predicate)
        if predicate_uri == str(RDF.type):
            continue
        entry = statements_by_predicate.setdefault(
            predicate_uri,
            {
                "predicate_label": _compact_uri(predicate_uri),
                "values": [],
            },
        )
        if isinstance(obj, URIRef):
            entry["values"].append(
                {
                    "label": _resolve_node_label(obj, label_map),
                    "uri": str(obj),
                    "meta": ", ".join(_node_type_labels(graph, obj)),
                }
            )
        else:
            parsed = _parse_statement_value(obj)
            entry["values"].append(
                {
                    "label": str(parsed["display"]),
                    "uri": parsed["uri"],
                    "meta": parsed["meta"],
                }
            )
    return [
        {"predicate_label": value["predicate_label"], "values": value["values"]}
        for _, value in sorted(statements_by_predicate.items(), key=lambda item: item[1]["predicate_label"])
    ]


def _build_metadata_rows(
    graph: Graph,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for predicate_uri, label in DESCRIBE_METADATA_LABELS.items():
        values: list[dict[str, str | None]] = []
        for obj in graph.objects(subject, URIRef(predicate_uri)):
            if isinstance(obj, URIRef):
                values.append(
                    {
                        "label": _resolve_node_label(obj, label_map),
                        "uri": str(obj),
                        "meta": ", ".join(_node_type_labels(graph, obj)) or None,
                    }
                )
            else:
                parsed = _parse_statement_value(obj)
                values.append(
                    {
                        "label": str(parsed["display"]),
                        "uri": parsed["uri"],
                        "meta": parsed["meta"],
                    }
                )
        if values:
            rows.append({"label": label, "values": values})
    return rows


def _section_title(graph: Graph, section: URIRef, label_map: dict[URIRef, str]) -> str:
    header_nodes = _ordered_uri_objects(graph, section, PATTERN_CONTAINS_HEADER)
    if header_nodes:
        return _resolve_node_label(header_nodes[0], label_map)
    return _resolve_node_label(section, label_map)


def _section_body(graph: Graph, section: URIRef, label_map: dict[URIRef, str]) -> str:
    paragraphs = _ordered_uri_objects(graph, section, PATTERN_CONTAINS)
    blocks: list[str] = []
    for paragraph in paragraphs:
        sentences = _ordered_uri_objects(graph, paragraph, PATTERN_CONTAINS)
        text = " ".join(_resolve_node_label(sentence, label_map) for sentence in sentences)
        text = _normalize_ws(text)
        if text:
            blocks.append(text)
    return "\n\n".join(blocks)


def _build_paper_sections(
    graph: Graph,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, str]]:
    sections: list[dict[str, str]] = []
    for section in _ordered_uri_objects(graph, subject, PATTERN_CONTAINS):
        if "/section/" not in str(section):
            continue
        title = _section_title(graph, section, label_map)
        body = _section_body(graph, section, label_map)
        type_labels = [label for label in _node_type_labels(graph, section) if label != "doco:Section"]
        sections.append(
            {
                "title": title,
                "kind": type_labels[0] if type_labels else "Section",
                "body": body,
            }
        )
    return sections


def _find_parent_section(graph: Graph, sentence: URIRef, label_map: dict[URIRef, str]) -> str | None:
    for paragraph in graph.subjects(URIRef(PATTERN_CONTAINS), sentence):
        if not isinstance(paragraph, URIRef):
            continue
        for section in graph.subjects(URIRef(PATTERN_CONTAINS), paragraph):
            if isinstance(section, URIRef) and "/section/" in str(section):
                return _section_title(graph, section, label_map)
    return None


def _build_provenance_cards(
    graph: Graph,
    subject: URIRef,
    label_map: dict[URIRef, str],
    excluded_nodes: set[URIRef] | None = None,
) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    blocked = excluded_nodes or set()
    evidence_nodes: set[URIRef] = {subject}
    evidence_nodes.update(
        candidate
        for candidate in graph.subjects(None, subject)
        if isinstance(candidate, URIRef)
    )
    for node in sorted(evidence_nodes, key=str):
        if node in blocked:
            continue
        source_papers = _ordered_uri_objects(graph, node, PROV_PRIMARY_SOURCE)
        derived_sentences = _ordered_uri_objects(graph, node, PROV_WAS_DERIVED_FROM)
        if not source_papers and not derived_sentences:
            continue
        relation_labels = sorted(
            {
                _compact_uri(str(predicate))
                for predicate in graph.predicates(node, subject)
                if node != subject
            }
        )
        sentence_label = _resolve_node_label(derived_sentences[0], label_map) if derived_sentences else None
        cards.append(
            {
                "label": _resolve_node_label(node, label_map),
                "kind": ", ".join(_node_type_labels(graph, node)),
                "relations": relation_labels,
                "paper_label": _resolve_node_label(source_papers[0], label_map) if source_papers else None,
                "paper_uri": str(source_papers[0]) if source_papers else None,
                "section_label": _find_parent_section(graph, derived_sentences[0], label_map) if derived_sentences else None,
                "sentence_label": sentence_label,
            }
        )
    return cards


def _find_parent_paragraph(graph: Graph, sentence: URIRef) -> URIRef | None:
    for paragraph in graph.subjects(URIRef(PATTERN_CONTAINS), sentence):
        if isinstance(paragraph, URIRef):
            return paragraph
    return None


def _find_parent_section_uri(graph: Graph, sentence: URIRef) -> URIRef | None:
    paragraph = _find_parent_paragraph(graph, sentence)
    if paragraph is None:
        return None
    for section in graph.subjects(URIRef(PATTERN_CONTAINS), paragraph):
        if isinstance(section, URIRef) and "/section/" in str(section):
            return section
    return None


def _type_badges(graph: Graph, node: URIRef, base_type: str) -> list[str]:
    labels: list[str] = []
    for type_uri in sorted({str(obj) for obj in graph.objects(node, RDF.type) if isinstance(obj, URIRef)}):
        compact = _compact_uri(type_uri)
        lowered = compact.lower()
        if lowered == base_type.lower():
            continue
        if compact == "doco:Section":
            continue
        labels.append(compact.removeprefix("sudo:"))
    return labels


def _highlight_fragments(text: str, needle: str) -> list[dict[str, object]]:
    clean_text = _normalize_ws(text)
    clean_needle = _normalize_ws(needle)
    if not clean_text or not clean_needle:
        return [{"text": clean_text, "match": False}]

    pattern = re.compile(re.escape(clean_needle), flags=re.IGNORECASE)
    matches = list(pattern.finditer(clean_text))
    if not matches:
        return [{"text": clean_text, "match": False}]

    fragments: list[dict[str, object]] = []
    cursor = 0
    for match in matches:
        start, end = match.span()
        if start > cursor:
            fragments.append({"text": clean_text[cursor:start], "match": False})
        fragments.append({"text": clean_text[start:end], "match": True})
        cursor = end
    if cursor < len(clean_text):
        fragments.append({"text": clean_text[cursor:], "match": False})
    return fragments


def _is_description_predicate(predicate_uri: str) -> bool:
    if predicate_uri in {str(RDF.type), PROV_PRIMARY_SOURCE, PROV_WAS_DERIVED_FROM, PATTERN_CONTAINS, PATTERN_CONTAINS_HEADER}:
        return False
    return predicate_uri.startswith((RDF_PREFIX, RDFS_PREFIX, SKOS_PREFIX, SCHEMA_PREFIX, DCTERMS_PREFIX, OWL_PREFIX))


def _is_rdf_structural_predicate(predicate_uri: str) -> bool:
    return predicate_uri.startswith((RDF_PREFIX, RDFS_PREFIX))


def _is_connected_resource_predicate(predicate_uri: str) -> bool:
    return predicate_uri.startswith(SUDO_PREFIX)


def _group_relation_entry(
    groups: dict[str, dict[str, object]],
    group_label: str,
    sort_order: int,
    item: dict[str, object],
) -> None:
    group = groups.setdefault(
        group_label,
        {
            "label": group_label,
            "sort_order": sort_order,
            "items": [],
            "seen": set(),
        },
    )
    seen = group["seen"]
    signature = json.dumps(item, sort_keys=True, default=str)
    if isinstance(seen, set) and signature in seen:
        return
    if isinstance(seen, set):
        seen.add(signature)
    items = group["items"]
    if isinstance(items, list):
        items.append(item)


def _predicate_group_label(predicate_uri: str) -> str:
    compact = _compact_uri(predicate_uri)
    return {
        "rdf:type": "Type",
        "rdfs:label": "Labels",
        "rdfs:comment": "Comments",
        "rdfs:seeAlso": "See Also",
        "rdfs:isDefinedBy": "Defined By",
        "skos:prefLabel": "Preferred Labels",
        "skos:closeMatch": "Concept Matches",
        "skos:exactMatch": "Concept Matches",
    }.get(compact, compact)


def _finalize_relation_groups(groups: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    finalized: list[dict[str, object]] = []
    for group in groups.values():
        items = group.get("items", [])
        if isinstance(items, list):
            items.sort(key=lambda item: str(item.get("label", "")).lower())
        finalized.append(
            {
                "label": group["label"],
                "items": items,
                "sort_order": group["sort_order"],
            }
        )
    finalized.sort(key=lambda group: (int(group["sort_order"]), str(group["label"]).lower()))
    for group in finalized:
        group.pop("sort_order", None)
    return finalized


def _graph_bucket(graph_identifier: object) -> str:
    if graph_identifier is None:
        return "default"
    graph_uri = str(graph_identifier).lower()
    for bucket in ("meta", "prov", "sudo", "concept"):
        if f"/{bucket}" in graph_uri or graph_uri.endswith(bucket) or f"#{bucket}" in graph_uri:
            return bucket
    if graph_uri == "urn:sudo:graph/default":
        return "default"
    return "default"


def _term_payload(
    graph: Graph,
    node: object,
    label_map: dict[URIRef, str],
) -> dict[str, object]:
    if isinstance(node, URIRef):
        return {
            "label": _resolve_node_label(node, label_map),
            "uri": str(node),
            "meta": ", ".join(_node_type_labels(graph, node)) or None,
        }
    parsed = _parse_statement_value(node)
    return {
        "label": str(parsed["display"]),
        "uri": parsed["uri"],
        "meta": parsed["meta"],
    }


def _build_source_statement_groups(
    graph: Graph,
    dataset: Dataset,
    subject: URIRef,
    label_map: dict[URIRef, str],
    include_statement: Callable[[str, str, str], bool],
    label_mode: str = "directional",
    show_source: bool = True,
) -> list[dict[str, object]]:
    groups: dict[str, dict[str, object]] = {}
    for statement_subject, predicate, obj, context in dataset.quads((None, None, None, None)):
        predicate_uri = str(predicate)
        source_bucket = _graph_bucket(context)
        if statement_subject == subject:
            if not include_statement("outgoing", predicate_uri, source_bucket):
                continue
            item = _term_payload(graph, obj, label_map)
            if show_source and source_bucket != "default":
                item["source"] = source_bucket
            group_label = _predicate_group_label(predicate_uri) if label_mode == "semantic" else _compact_uri(predicate_uri)
            _group_relation_entry(groups, group_label, 0, item)
            continue

        if obj == subject and isinstance(statement_subject, URIRef):
            if not include_statement("incoming", predicate_uri, source_bucket):
                continue
            item = _term_payload(graph, statement_subject, label_map)
            if show_source and source_bucket != "default":
                item["source"] = source_bucket
            group_label = _predicate_group_label(predicate_uri) if label_mode == "semantic" else _compact_uri(predicate_uri)
            _group_relation_entry(groups, group_label, 1, item)
    return _finalize_relation_groups(groups)


def _build_description_groups(
    graph: Graph,
    dataset: Dataset,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    return _build_source_statement_groups(
        graph=graph,
        dataset=dataset,
        subject=subject,
        label_map=label_map,
        include_statement=lambda _direction, predicate_uri, source_bucket: (
            source_bucket == "meta"
            or (
                _is_rdf_structural_predicate(predicate_uri)
                and source_bucket != "concept"
            )
        ),
        label_mode="semantic",
        show_source=True,
    )


def _build_artifact_references_for_nodes(
    graph: Graph,
    artifacts: list[URIRef],
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    paper_index: dict[str, dict[str, object]] = {}
    for artifact in artifacts:
        artifact_label = _resolve_node_label(artifact, label_map)
        evidence_nodes = sorted(
            {
                candidate
                for candidate in graph.subjects(None, artifact)
                if isinstance(candidate, URIRef) and _classify_describe_kind(_node_type_labels(graph, candidate)) in {"argument", "descriptor"}
            }
            | {
                candidate
                for _, candidate in graph.predicate_objects(artifact)
                if isinstance(candidate, URIRef) and _classify_describe_kind(_node_type_labels(graph, candidate)) in {"argument", "descriptor"}
            },
            key=lambda node: (_sort_key_for_uri(node, graph), str(node)),
        )

        for node in evidence_nodes:
            relation_labels = sorted(
                {
                    _compact_uri(str(predicate)).removeprefix("sudo:")
                    for predicate in graph.predicates(node, artifact)
                }
                | {
                    _compact_uri(str(predicate)).removeprefix("sudo:")
                    for predicate in graph.predicates(artifact, node)
                }
            )
            source_paper = next(iter(_ordered_uri_objects(graph, node, PROV_PRIMARY_SOURCE)), None)
            sentence = next(iter(_ordered_uri_objects(graph, node, PROV_WAS_DERIVED_FROM)), None)
            section = _find_parent_section_uri(graph, sentence) if sentence else None
            section_title = _section_title(graph, section, label_map) if section else "Unplaced reference"
            section_kind_labels = _type_badges(graph, section, "doco:section") if section else []
            paper_key = str(source_paper or "unknown-paper")
            paper_entry = paper_index.setdefault(
                paper_key,
                {
                    "paper_label": _resolve_node_label(source_paper, label_map) if source_paper else "Source paper not available",
                    "paper_uri": str(source_paper) if source_paper else None,
                    "sections": {},
                },
            )
            section_key = str(section or "unknown-section")
            sections = paper_entry["sections"]
            if isinstance(sections, dict):
                section_entry = sections.setdefault(
                    section_key,
                    {
                        "title": section_title,
                        "kind": ", ".join(section_kind_labels) if section_kind_labels else "Section",
                        "items": [],
                        "sort_key": _sort_key_for_uri(section, graph) if section else (10**9, section_title),
                    },
                )
                items = section_entry["items"]
                if isinstance(items, list):
                    entry = {
                        "node_label": _resolve_node_label(node, label_map),
                        "node_uri": str(node),
                        "artifact_label": artifact_label,
                        "artifact_uri": str(artifact),
                        "kind": ", ".join(_type_badges(graph, node, "sudo:argument" if "sudo:Argument" in _node_type_labels(graph, node) else "sudo:descriptor")),
                        "relation_labels": relation_labels,
                        "sentence_position": _sort_key_for_uri(sentence, graph)[0] if sentence else None,
                        "fragments": _highlight_fragments(_resolve_node_label(node, label_map), artifact_label),
                    }
                    signature = json.dumps(entry, sort_keys=True, default=str)
                    existing = {
                        json.dumps(candidate, sort_keys=True, default=str)
                        for candidate in items
                        if isinstance(candidate, dict)
                    }
                    if signature not in existing:
                        items.append(entry)

    papers: list[dict[str, object]] = []
    for paper_entry in paper_index.values():
        sections = paper_entry.get("sections", {})
        if not isinstance(sections, dict):
            continue
        ordered_sections = sorted(sections.values(), key=lambda item: item["sort_key"])
        for section in ordered_sections:
            items = section.get("items", [])
            if isinstance(items, list):
                items.sort(key=lambda item: (item["sentence_position"] or 10**9, str(item["node_label"])))
        papers.append(
            {
                "paper_label": paper_entry["paper_label"],
                "paper_uri": paper_entry["paper_uri"],
                "sections": ordered_sections,
            }
        )

    papers.sort(key=lambda item: str(item["paper_label"]).lower())
    return papers


def _build_artifact_references(
    graph: Graph,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    return _build_artifact_references_for_nodes(graph, [subject], label_map)


def _build_relation_groups(
    graph: Graph,
    dataset: Dataset,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    return _build_source_statement_groups(
        graph=graph,
        dataset=dataset,
        subject=subject,
        label_map=label_map,
        include_statement=lambda _direction, predicate_uri, source_bucket: (
            source_bucket == "concept"
            or (
                source_bucket == "sudo"
                and not _is_rdf_structural_predicate(predicate_uri)
            )
        ),
        label_mode="semantic",
        show_source=False,
    )


def _build_provenance_groups(
    graph: Graph,
    dataset: Dataset,
    subject: URIRef,
    label_map: dict[URIRef, str],
) -> list[dict[str, object]]:
    return _build_source_statement_groups(
        graph=graph,
        dataset=dataset,
        subject=subject,
        label_map=label_map,
        include_statement=lambda _direction, _predicate_uri, source_bucket: source_bucket == "prov",
        label_mode="semantic",
        show_source=True,
    )


def _topic_artifact_nodes(graph: Graph, subject: URIRef) -> list[URIRef]:
    nodes = {
        node
        for _, node in graph.predicate_objects(subject)
        if isinstance(node, URIRef) and _classify_describe_kind(_node_type_labels(graph, node)) == "artifact"
    } | {
        node
        for node in graph.subjects(None, subject)
        if isinstance(node, URIRef) and _classify_describe_kind(_node_type_labels(graph, node)) == "artifact"
    }
    return sorted(nodes, key=lambda node: (_sort_key_for_uri(node, graph), str(node)))


def _build_describe_view(
    root_graph: Graph,
    connected_graphs: list[Graph],
    subject_dataset: Dataset,
    connected_node_count: int,
    resource_path: str,
    display_node_type: str,
    display_local_id: str,
    persistent_uri: str,
) -> dict[str, object]:
    combined_graph = _merge_graphs([root_graph, *connected_graphs])
    subject = URIRef(persistent_uri)
    label_map = _build_label_map(combined_graph)
    title = _resolve_node_label(subject, label_map)
    description = None
    for predicate_uri in DESCRIPTION_PREDICATES:
        for obj in combined_graph.objects(subject, URIRef(predicate_uri)):
            if isinstance(obj, Literal):
                description = _normalize_ws(str(obj))
                break
        if description:
            break
    type_labels = _node_type_labels(combined_graph, subject)
    describe_kind = _classify_describe_kind(type_labels)
    is_topic_node = _is_topic_node(type_labels)
    metadata_rows = _build_metadata_rows(combined_graph, subject, label_map)
    description_groups = _build_description_groups(combined_graph, subject_dataset, subject, label_map)
    paper_sections = _build_paper_sections(combined_graph, subject, label_map) if describe_kind == "paper" else []
    topic_artifact_nodes = _topic_artifact_nodes(combined_graph, subject) if is_topic_node else []
    provenance_cards = _build_provenance_cards(
        combined_graph,
        subject,
        label_map,
        excluded_nodes=set(topic_artifact_nodes) if is_topic_node else None,
    )
    if describe_kind == "artifact":
        artifact_references = _build_artifact_references(combined_graph, subject, label_map)
    elif is_topic_node and topic_artifact_nodes:
        artifact_references = _build_artifact_references_for_nodes(combined_graph, topic_artifact_nodes, label_map)
    else:
        artifact_references = []
    relation_groups = _build_relation_groups(combined_graph, subject_dataset, subject, label_map)
    provenance_groups = _build_provenance_groups(combined_graph, subject_dataset, subject, label_map)
    return {
        "title": title or resource_path,
        "description": description,
        "node_type": display_node_type,
        "local_id": display_local_id,
        "persistent_uri": persistent_uri,
        "type_labels": type_labels,
        "describe_kind": describe_kind,
        "is_topic_node": is_topic_node,
        "metadata_rows": metadata_rows,
        "description_groups": description_groups,
        "paper_sections": paper_sections,
        "provenance_groups": provenance_groups,
        "provenance_cards": provenance_cards,
        "artifact_references": artifact_references,
        "relation_groups": relation_groups,
        "connected_node_count": connected_node_count,
        "statement_count": sum(len(row["values"]) for row in metadata_rows) + sum(len(group["items"]) for group in description_groups),
    }


def _resource_path_from_persistent_uri(uri: str, persistent_uri_base: str) -> str | None:
    normalized_base = persistent_uri_base.rstrip("/")
    if not uri.startswith(normalized_base):
        return None
    resource_path = uri[len(normalized_base) :].lstrip("/")
    return resource_path or None


def _build_graph_view(
    root_graph: Graph,
    connected_graphs: list[Graph],
    neighborhood: dict[URIRef, dict[str, set[str]]],
    resource_path: str,
    display_node_type: str,
    display_local_id: str,
    persistent_uri: str,
    persistent_uri_base: str,
) -> dict[str, object]:
    combined_graph = _merge_graphs([root_graph, *connected_graphs])
    subject = URIRef(persistent_uri)
    label_map = _build_label_map(combined_graph)
    title = _resolve_node_label(subject, label_map)
    description = None
    for predicate_uri in DESCRIPTION_PREDICATES:
        for obj in combined_graph.objects(subject, URIRef(predicate_uri)):
            if isinstance(obj, Literal):
                description = _normalize_ws(str(obj))
                break
        if description:
            break

    def _node_direction(entry: dict[str, set[str]]) -> str:
        has_incoming = bool(entry["incoming"])
        has_outgoing = bool(entry["outgoing"])
        if has_incoming and has_outgoing:
            return "bidirectional"
        if has_incoming:
            return "incoming"
        if has_outgoing:
            return "outgoing"
        return "neutral"

    def _node_payload(node: URIRef, role: str, relation_entry: dict[str, set[str]] | None = None) -> dict[str, object]:
        return {
            "id": str(node),
            "label": _resolve_node_label(node, label_map),
            "role": role,
            "direction": _node_direction(relation_entry) if relation_entry is not None else "root",
            "types": [label.removeprefix("sudo:") for label in _node_type_labels(combined_graph, node)],
            "uri": str(node),
            "resource_path": _resource_path_from_persistent_uri(str(node), persistent_uri_base),
        }

    nodes = [_node_payload(subject, "root")]
    edges: list[dict[str, object]] = []

    for node in sorted(neighborhood.keys(), key=lambda item: _resolve_node_label(item, label_map).lower()):
        relation_entry = neighborhood[node]
        nodes.append(_node_payload(node, "neighbor", relation_entry))
        for direction in ("incoming", "outgoing"):
            predicate_uris = relation_entry[direction]
            if not predicate_uris:
                continue
            relation_labels = sorted(_compact_uri(uri) for uri in predicate_uris)
            source_node = str(node) if direction == "incoming" else str(subject)
            target_node = str(subject) if direction == "incoming" else str(node)
            edge_label = relation_labels[0]
            if len(relation_labels) > 1:
                edge_label = f"{edge_label} +{len(relation_labels) - 1}"
            edges.append(
                {
                    "source": source_node,
                    "target": target_node,
                    "direction": direction,
                    "relation_labels": relation_labels,
                    "label": edge_label,
                }
            )

    return {
        "title": title or resource_path,
        "description": description,
        "node_type": display_node_type,
        "local_id": display_local_id,
        "persistent_uri": persistent_uri,
        "type_labels": _node_type_labels(combined_graph, subject),
        "graph_nodes": nodes,
        "graph_edges": edges,
        "graph_node_count": len(nodes),
        "graph_edge_count": len(edges),
        "connected_node_count": len(neighborhood),
    }


def _parse_statement_value(value: object) -> dict[str, str | bool | None]:
    if isinstance(value, URIRef):
        return {
            "is_uri": True,
            "uri": str(value),
            "display": _compact_uri(str(value)),
            "meta": None,
        }

    if isinstance(value, BNode):
        return {
            "is_uri": False,
            "uri": None,
            "display": f"_:{value}",
            "meta": None,
        }

    if isinstance(value, Literal):
        language = value.language
        datatype = str(value.datatype) if value.datatype else None
        raw_value = str(value)
    else:
        language = None
        datatype = None
        raw_value = str(value)

    meta: str | None = None
    if language:
        meta = f"@{language}"
    elif datatype:
        meta = _compact_uri(datatype)

    return {
        "is_uri": False,
        "uri": None,
        "display": raw_value,
        "meta": meta,
    }


def build_doc_view(
    graph: Graph,
    resource_path: str,
    display_node_type: str,
    display_local_id: str,
    persistent_uri: str,
) -> dict[str, object]:
    statements_by_predicate: dict[str, dict[str, object]] = {}
    title: str | None = None
    description: str | None = None
    subject = URIRef(persistent_uri)

    for predicate, obj in graph.predicate_objects(subject):
        predicate_uri = str(predicate)
        parsed_value = _parse_statement_value(obj)
        if predicate_uri not in statements_by_predicate:
            statements_by_predicate[predicate_uri] = {
                "predicate_uri": predicate_uri,
                "predicate_label": _compact_uri(predicate_uri),
                "values": [],
            }
        values = statements_by_predicate[predicate_uri]["values"]
        if isinstance(values, list):
            values.append(parsed_value)

        if title is None and predicate_uri in TITLE_PREDICATES and not parsed_value["is_uri"]:
            candidate = parsed_value["display"]
            if isinstance(candidate, str) and candidate.strip():
                title = candidate.strip()

        if description is None and predicate_uri in DESCRIPTION_PREDICATES and not parsed_value["is_uri"]:
            candidate = parsed_value["display"]
            if isinstance(candidate, str) and candidate.strip():
                description = candidate.strip()

    sorted_statements = sorted(
        statements_by_predicate.values(),
        key=lambda item: str(item.get("predicate_label", "")),
    )

    return {
        "title": title or resource_path,
        "description": description,
        "node_type": display_node_type,
        "local_id": display_local_id,
        "persistent_uri": persistent_uri,
        "statement_count": sum(
            len(item.get("values", [])) if isinstance(item.get("values"), list) else 0
            for item in sorted_statements
        ),
        "statements": sorted_statements,
    }


def build_rdf_view(
    graph: Graph,
    resource_path: str,
    display_node_type: str,
    display_local_id: str,
    persistent_uri: str,
    source_content_type: str,
    mode: str,
) -> dict[str, object]:
    doc_view = build_doc_view(
        graph=graph,
        resource_path=resource_path,
        display_node_type=display_node_type,
        display_local_id=display_local_id,
        persistent_uri=persistent_uri,
    )
    rdf_text = _serialize_graph(graph, fmt="turtle").strip()
    return {
        **doc_view,
        "rdf_text": rdf_text,
        "rdf_line_count": len(rdf_text.splitlines()) if rdf_text else 0,
        "source_content_type": source_content_type,
        "view_mode": mode,
    }


def _cache_key(resource_path: str, view_mode: str) -> str:
    return f"{resource_path.strip('/')}::{view_mode}"


def _increment_query_counter(counter: dict[str, int] | None, amount: int = 1) -> None:
    if counter is None:
        return
    counter["total"] = counter.get("total", 0) + amount


def _is_empty_doc_graph(graph: Graph, persistent_uri: str) -> bool:
    return next(graph.predicate_objects(URIRef(persistent_uri)), None) is None


def _fetch_doc_graph_from_datasets(
    backend: FusekiRedirectBackend,
    resource_label: str,
    persistent_uri: str,
    dataset_names: list[str],
    query_counter: dict[str, int] | None = None,
) -> tuple[Graph, str, str, str] | None:
    last_error: Exception | None = None
    saw_success = False
    for dataset_name in dataset_names:
        source_url = backend.get_doc_target(
            resource_label=resource_label,
            persistent_uri=persistent_uri,
            dataset=dataset_name,
        )
        _increment_query_counter(query_counter)
        try:
            graph, source_content_type = fetch_doc_graph(source_url)
        except RuntimeError as exc:
            last_error = exc
            continue

        saw_success = True
        if _is_empty_doc_graph(graph, persistent_uri):
            continue

        return graph, source_content_type, source_url, dataset_name

    if last_error is not None and not saw_success:
        raise RuntimeError(str(last_error)) from last_error

    return None


def create_app(
    settings: AppSettings | None = None,
    dataset_names_provider: Callable[[str], list[str]] | None = None,
) -> FastAPI:
    app_settings = settings or AppSettings()
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)
    logging.getLogger("uri_resolver").setLevel(logging.INFO)

    base_dir = Path(__file__).parent
    templates = Jinja2Templates(directory=str(base_dir / "templates"))

    backend = FusekiRedirectBackend(
        server_url=app_settings.fuseki_server_url,
        dataset=app_settings.fuseki_dataset,
    )
    resolver = ResolverService(
        backend=backend,
        persistent_uri_base=app_settings.persistent_uri_base,
    )

    app = FastAPI(title="Scholarly KG URI Resolver", version="0.1.0")
    rdf_view_cache: OrderedDict[str, dict[str, object]] = OrderedDict()
    rdf_view_cache_limit = 256
    persistent_uri_path = urlsplit(app_settings.persistent_uri_base).path.rstrip("/")
    if app_settings.public_base_path:
        app.add_middleware(
            _StripPublicBasePathMiddleware,
            public_base_path=app_settings.public_base_path,
        )
    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")
    if app_settings.public_base_path:
        app.mount(
            f"{app_settings.public_base_path}/static",
            StaticFiles(directory=str(base_dir / "static")),
            name="static_prefixed",
        )

    logger.info(
        "resolver_config public_base_path=%s",
        app_settings.public_base_path or "<root>",
    )

    @app.middleware("http")
    async def log_incoming_request(request: Request, call_next):
        body_bytes = await request.body()

        # Restore the body so downstream handlers can read it
        async def receive():
            return {"type": "http.request", "body": body_bytes}

        request._receive = receive

        # Decode payload safely
        try:
            payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else None
        except Exception:
            payload = body_bytes.decode("utf-8", errors="replace")

        logging.info({
            "method": request.method,
            "url": str(request.url),
            "client": request.client.host if request.client else None,
            "payload": payload,
        })

        response = await call_next(request)
        return response

    def _dataset_candidates() -> list[str]:
        discovered = (
            dataset_names_provider(app_settings.fuseki_server_url)
            if dataset_names_provider is not None
            else fetch_fuseki_dataset_names(app_settings.fuseki_server_url)
        )
        return _ordered_dataset_names(app_settings.fuseki_dataset, discovered)

    def _public_base_path_for_request(request: Request) -> str:
        # Support proxy-provided prefix and fallback to configured env value.
        forwarded_prefix = _normalize_public_base_path(
            request.headers.get("x-forwarded-prefix", "").split(",", 1)[0]
        )
        if forwarded_prefix:
            return forwarded_prefix

        for header_name in ("x-original-uri", "x-forwarded-uri"):
            inferred = _infer_public_base_path_from_uri_header(request, header_name)
            if inferred:
                return inferred

        return app_settings.public_base_path

    def _public_path_for(request: Request, route_name: str, **path_params: str) -> str:
        route_path = str(app.url_path_for(route_name, **path_params))
        return _join_public_base_path(_public_base_path_for_request(request), route_path)

    def _static_path_for_request(request: Request, asset_path: str) -> str:
        return _static_path_for(_public_base_path_for_request(request), asset_path)

    def _cache_rdf_view(resource_path: str, view_mode: str, payload: dict[str, object]) -> None:
        cache_entry = dict(payload)
        key = _cache_key(resource_path, view_mode)
        rdf_view_cache[key] = cache_entry
        rdf_view_cache.move_to_end(key)
        while len(rdf_view_cache) > rdf_view_cache_limit:
            rdf_view_cache.popitem(last=False)

    def _get_cached_rdf_view(resource_path: str, view_mode: str) -> dict[str, object] | None:
        key = _cache_key(resource_path, view_mode)
        cached = rdf_view_cache.get(key)
        if cached is None:
            return None
        rdf_view_cache.move_to_end(key)
        return dict(cached)

    def _build_resolution_response(request: Request, resource_path: str) -> RedirectResponse:
        resource_type, local_id = _resource_parts(resource_path)
        try:
            choice = resolver.choose_id_representation(request.headers.get("accept"))
        except NotAcceptableError as exc:
            raise HTTPException(status_code=406, detail=str(exc)) from exc

        if choice.kind == "doc":
            location = _public_path_for(
                request,
                "get_doc_path",
                resource_type=resource_type,
                local_id=local_id,
            )
        else:
            location = _public_path_for(
                request,
                "get_data_format_path",
                resource_type=resource_type,
                local_id=local_id,
                fmt=choice.fmt,
            )

        response = RedirectResponse(url=location, status_code=303)
        response.headers["Vary"] = "Accept"
        return response

    def _dereference_resource_response(
        request: Request,
        resource_path: str,
    ) -> Response:
        resource_type, local_id = _resource_parts(resource_path)
        try:
            choice = resolver.choose_id_representation(request.headers.get("accept"))
        except NotAcceptableError as exc:
            raise HTTPException(status_code=406, detail=str(exc)) from exc

        if choice.kind == "doc":
            return _doc_render_response(request, resource_path)

        return _data_redirect_response(
            request=request,
            resource_path=_resource_path(resource_type, local_id),
            media_type=choice.media_type,
            fmt=choice.fmt or MEDIA_TYPE_TO_FORMAT[choice.media_type],
        )

    @app.get("/id/{node_type}/{local_id}", name="resolve_id")
    async def resolve_id(request: Request, node_type: NodeType, local_id: str) -> RedirectResponse:
        return _build_resolution_response(request, _resource_path(node_type.value, local_id))

    @app.get("/{resource_type}/{local_id}", name="resolve_generic_id")
    async def resolve_generic_id(request: Request, resource_type: str, local_id: str) -> RedirectResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        return _build_resolution_response(request, _resource_path(resource_type, local_id))

    if persistent_uri_path:
        @app.get(f"{persistent_uri_path}/{{node_type}}/{{local_id}}", name="resolve_canonical_id")
        async def resolve_canonical_id(
            request: Request,
            node_type: NodeType,
            local_id: str,
        ) -> Response:
            return _dereference_resource_response(request, _resource_path(node_type.value, local_id))

        @app.get(f"{persistent_uri_path}/{{resource_type}}/{{local_id}}", name="resolve_canonical_generic_id")
        async def resolve_canonical_generic_id(
            request: Request,
            resource_type: str,
            local_id: str,
        ) -> Response:
            if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
                raise HTTPException(status_code=404, detail="Not found")
            return _dereference_resource_response(request, _resource_path(resource_type, local_id))

    @app.get("/resolve", name="resolve_form")
    async def resolve_form(request: Request, node_type: NodeType, local_id: str) -> RedirectResponse:
        normalized_local_id = _normalize_local_id(local_id)
        if not normalized_local_id:
            raise HTTPException(
                status_code=400,
                detail="local_id must contain at least one letter or digit",
            )

        location = _public_path_for(
            request,
            "resolve_id",
            node_type=node_type.value,
            local_id=normalized_local_id,
        )
        return RedirectResponse(url=location, status_code=303)

    def _doc_render_response(request: Request, resource_path: str) -> HTMLResponse:
        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        try:
            resolved = _fetch_doc_graph_from_datasets(
                backend=backend,
                resource_label=resource_path,
                persistent_uri=persistent_uri,
                dataset_names=_dataset_candidates(),
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if resolved is None:
            logger.info(
                "entity_not_found resource=%s persistent_uri=%s",
                resource_path,
                persistent_uri,
            )
            resource_type, local_id = _resource_parts(resource_path)
            not_found_path = _public_path_for(
                request,
                "get_not_found_path",
                resource_type=resource_type,
                local_id=local_id,
            )
            return RedirectResponse(url=not_found_path, status_code=303)

        graph, source_content_type, source_url, _dataset_name = resolved
        logger.info(
            "doc_rendered resource=%s persistent_uri=%s source_url=%s",
            resource_path,
            persistent_uri,
            source_url,
        )

        display_type, display_local_id = _resource_parts(resource_path)
        view = build_doc_view(
            graph=graph,
            resource_path=resource_path,
            display_node_type=display_type,
            display_local_id=display_local_id,
            persistent_uri=persistent_uri,
        )
        _cache_rdf_view(
            resource_path,
            "preview",
            {
                **build_rdf_view(
                    graph=graph,
                    resource_path=resource_path,
                    display_node_type=display_type,
                    display_local_id=display_local_id,
                    persistent_uri=persistent_uri,
                    source_content_type=source_content_type,
                    mode="preview",
                ),
                "source_url": source_url,
                "sparql_path": source_url,
            },
        )
        response = templates.TemplateResponse(
            request=request,
            name="resource.html",
            context={
                **view,
                "css_path": _static_path_for_request(request, "doc.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "chrome_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-512x512.png",
                ),
                "doc_path": _public_path_for(
                    request,
                    "get_doc_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "describe_path": _public_path_for(
                    request,
                    "get_describe_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "graph_path": _public_path_for(
                    request,
                    "get_graph_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "rdf_path": _public_path_for(
                    request,
                    "get_rdf_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ) + "?view=preview",
                "data_path": _public_path_for(
                    request,
                    "get_data_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "data_ttl_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="ttl",
                ),
                "data_jsonld_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="jsonld",
                ),
                "data_rdf_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="rdf",
                ),
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
                "persistent_uri_path": persistent_uri,
                "source_url": source_url,
                "sparql_path": source_url,
                "source_content_type": source_content_type,
            },
        )
        response.headers["Vary"] = "Accept"
        return response

    def _rdf_render_response(request: Request, resource_path: str, view_mode: str) -> HTMLResponse:
        cached_view = _get_cached_rdf_view(resource_path, view_mode)
        if cached_view is not None:
            display_type, display_local_id = _resource_parts(resource_path)
            response = templates.TemplateResponse(
                request=request,
                name="rdf.html",
                context={
                    **cached_view,
                    "css_path": _static_path_for_request(request, "doc.css"),
                    "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                    "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                    "apple_touch_icon_path": _static_path_for_request(
                        request,
                        "images/android-chrome-192x192.png",
                    ),
                    "doc_path": _public_path_for(
                        request,
                        "get_doc_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                    ),
                    "describe_path": _public_path_for(
                        request,
                        "get_describe_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                    ),
                    "graph_path": _public_path_for(
                        request,
                        "get_graph_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                    ),
                    "rdf_path": _public_path_for(
                        request,
                        "get_rdf_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                    ) + f"?view={view_mode}",
                    "data_path": _public_path_for(
                        request,
                        "get_data_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                    ),
                    "data_ttl_path": _public_path_for(
                        request,
                        "get_data_format_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                        fmt="ttl",
                    ),
                    "data_jsonld_path": _public_path_for(
                        request,
                        "get_data_format_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                        fmt="jsonld",
                    ),
                    "data_rdf_path": _public_path_for(
                        request,
                        "get_data_format_path",
                        resource_type=display_type,
                        local_id=display_local_id,
                        fmt="rdf",
                    ),
                    "home_path": _join_public_base_path(
                        _public_base_path_for_request(request),
                        str(app.url_path_for("root")),
                    ),
                },
            )
            response.headers["Vary"] = "Accept"
            return response

        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        try:
            resolved = _fetch_doc_graph_from_datasets(
                backend=backend,
                resource_label=resource_path,
                persistent_uri=persistent_uri,
                dataset_names=_dataset_candidates(),
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if resolved is None:
            logger.info(
                "entity_not_found resource=%s persistent_uri=%s",
                resource_path,
                persistent_uri,
            )
            resource_type, local_id = _resource_parts(resource_path)
            not_found_path = _public_path_for(
                request,
                "get_not_found_path",
                resource_type=resource_type,
                local_id=local_id,
            )
            return RedirectResponse(url=not_found_path, status_code=303)

        root_graph, source_content_type, source_url, _dataset_name = resolved
        render_graph = root_graph
        if view_mode == "describe":
            related_graphs = _fetch_related_graphs(
                backend=backend,
                persistent_uri_base=resolver.persistent_uri_base,
                graph=root_graph,
                root_uri=URIRef(persistent_uri),
                dataset_names=_dataset_candidates(),
            )
            render_graph = _merge_graphs([root_graph, *related_graphs])

        display_type, display_local_id = _resource_parts(resource_path)
        view = build_rdf_view(
            graph=render_graph,
            resource_path=resource_path,
            display_node_type=display_type,
            display_local_id=display_local_id,
            persistent_uri=persistent_uri,
            source_content_type=source_content_type,
            mode=view_mode,
        )
        _cache_rdf_view(
            resource_path,
            view_mode,
            {
                **view,
                "source_url": source_url,
                "sparql_path": source_url,
            },
        )
        response = templates.TemplateResponse(
            request=request,
            name="rdf.html",
            context={
                **view,
                "css_path": _static_path_for_request(request, "doc.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "doc_path": _public_path_for(
                    request,
                    "get_doc_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "describe_path": _public_path_for(
                    request,
                    "get_describe_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "graph_path": _public_path_for(
                    request,
                    "get_graph_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "rdf_path": _public_path_for(
                    request,
                    "get_rdf_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ) + f"?view={view_mode}",
                "data_path": _public_path_for(
                    request,
                    "get_data_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "data_ttl_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="ttl",
                ),
                "data_jsonld_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="jsonld",
                ),
                "data_rdf_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="rdf",
                ),
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
                "persistent_uri_path": persistent_uri,
                "source_url": source_url,
                "source_content_type": source_content_type,
                "sparql_path": source_url,
            },
        )
        response.headers["Vary"] = "Accept"
        return response

    def _resolve_graph_bundle(
        resource_path: str,
        query_counter: dict[str, int] | None = None,
    ) -> dict[str, object] | None:
        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        resolved = _fetch_doc_graph_from_datasets(
            backend=backend,
            resource_label=resource_path,
            persistent_uri=persistent_uri,
            dataset_names=_dataset_candidates(),
            query_counter=query_counter,
        )
        if resolved is None:
            return None

        root_graph, source_content_type, source_url, _dataset_name = resolved
        dataset_names = _dataset_candidates()
        neighborhood = _node_neighborhood_map(
            backend=backend,
            dataset_names=dataset_names,
            target_uri=URIRef(persistent_uri),
            query_counter=query_counter,
        )
        subject_dataset = _fetch_quad_dataset_from_datasets(
            backend=backend,
            resource_label=resource_path,
            query=_incident_quads_query(URIRef(persistent_uri)),
            dataset_names=dataset_names,
            query_counter=query_counter,
        )
        related_graphs = _fetch_related_graphs(
            backend=backend,
            persistent_uri_base=resolver.persistent_uri_base,
            graph=root_graph,
            root_uri=URIRef(persistent_uri),
            dataset_names=dataset_names,
            query_counter=query_counter,
            neighborhood=neighborhood,
        )
        return {
            "persistent_uri": persistent_uri,
            "root_graph": root_graph,
            "source_content_type": source_content_type,
            "source_url": source_url,
            "subject_dataset": subject_dataset,
            "related_graphs": related_graphs,
            "neighborhood": neighborhood,
        }

    def _describe_render_response(request: Request, resource_path: str) -> HTMLResponse:
        query_counter = {"total": 0}
        try:
            bundle = _resolve_graph_bundle(resource_path, query_counter=query_counter)
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        if bundle is None:
            logger.info(
                "entity_not_found resource=%s persistent_uri=%s",
                resource_path,
                persistent_uri,
            )
            resource_type, local_id = _resource_parts(resource_path)
            not_found_path = _public_path_for(
                request,
                "get_not_found_path",
                resource_type=resource_type,
                local_id=local_id,
            )
            return RedirectResponse(url=not_found_path, status_code=303)

        display_type, display_local_id = _resource_parts(resource_path)
        view = _build_describe_view(
            root_graph=bundle["root_graph"],
            connected_graphs=bundle["related_graphs"],
            subject_dataset=bundle["subject_dataset"],
            connected_node_count=len(bundle["neighborhood"]),
            resource_path=resource_path,
            display_node_type=display_type,
            display_local_id=display_local_id,
            persistent_uri=bundle["persistent_uri"],
        )
        _cache_rdf_view(
            resource_path,
            "describe",
            {
                **build_rdf_view(
                    graph=_merge_graphs([bundle["root_graph"], *bundle["related_graphs"]]),
                    resource_path=resource_path,
                    display_node_type=display_type,
                    display_local_id=display_local_id,
                    persistent_uri=bundle["persistent_uri"],
                    source_content_type=bundle["source_content_type"],
                    mode="describe",
                ),
                "source_url": bundle["source_url"],
                "sparql_path": bundle["source_url"],
            },
        )
        response = templates.TemplateResponse(
            request=request,
            name="describe.html",
            context={
                **view,
                "css_path": _static_path_for_request(request, "doc.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "doc_path": _public_path_for(
                    request,
                    "get_doc_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "describe_path": _public_path_for(
                    request,
                    "get_describe_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "graph_path": _public_path_for(
                    request,
                    "get_graph_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "rdf_path": _public_path_for(
                    request,
                    "get_rdf_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ) + "?view=describe",
                "data_path": _public_path_for(
                    request,
                    "get_data_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "data_ttl_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="ttl",
                ),
                "data_jsonld_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="jsonld",
                ),
                "data_rdf_path": _public_path_for(
                    request,
                    "get_data_format_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                    fmt="rdf",
                ),
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
                "persistent_uri_path": bundle["persistent_uri"],
                "source_url": bundle["source_url"],
                "sparql_path": bundle["source_url"],
                "sparql_query_count": query_counter["total"],
                "source_content_type": bundle["source_content_type"],
            },
        )
        response.headers["Vary"] = "Accept"
        return response

    def _graph_render_response(request: Request, resource_path: str) -> HTMLResponse:
        query_counter = {"total": 0}
        try:
            bundle = _resolve_graph_bundle(resource_path, query_counter=query_counter)
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        if bundle is None:
            logger.info(
                "entity_not_found resource=%s persistent_uri=%s",
                resource_path,
                persistent_uri,
            )
            resource_type, local_id = _resource_parts(resource_path)
            not_found_path = _public_path_for(
                request,
                "get_not_found_path",
                resource_type=resource_type,
                local_id=local_id,
            )
            return RedirectResponse(url=not_found_path, status_code=303)

        display_type, display_local_id = _resource_parts(resource_path)
        view = _build_graph_view(
            root_graph=bundle["root_graph"],
            connected_graphs=bundle["related_graphs"],
            neighborhood=bundle["neighborhood"],
            resource_path=resource_path,
            display_node_type=display_type,
            display_local_id=display_local_id,
            persistent_uri=bundle["persistent_uri"],
            persistent_uri_base=resolver.persistent_uri_base,
        )
        _cache_rdf_view(
            resource_path,
            "describe",
            {
                **build_rdf_view(
                    graph=_merge_graphs([bundle["root_graph"], *bundle["related_graphs"]]),
                    resource_path=resource_path,
                    display_node_type=display_type,
                    display_local_id=display_local_id,
                    persistent_uri=bundle["persistent_uri"],
                    source_content_type=bundle["source_content_type"],
                    mode="describe",
                ),
                "source_url": bundle["source_url"],
                "sparql_path": bundle["source_url"],
            },
        )

        graph_nodes: list[dict[str, object]] = []
        for node in view["graph_nodes"]:
            resource_link = node.get("resource_path")
            if isinstance(resource_link, str) and "/" in resource_link:
                node_type, local_id = _resource_parts(resource_link)
                href = _public_path_for(
                    request,
                    "get_doc_path",
                    resource_type=node_type,
                    local_id=local_id,
                )
            else:
                href = str(node.get("uri") or "#")
            graph_nodes.append({**node, "href": href})

        response = templates.TemplateResponse(
            request=request,
            name="graph.html",
            context={
                **view,
                "graph_nodes": graph_nodes,
                "css_path": _static_path_for_request(request, "doc.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "doc_path": _public_path_for(
                    request,
                    "get_doc_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "describe_path": _public_path_for(
                    request,
                    "get_describe_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "graph_path": _public_path_for(
                    request,
                    "get_graph_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ),
                "rdf_path": _public_path_for(
                    request,
                    "get_rdf_path",
                    resource_type=display_type,
                    local_id=display_local_id,
                ) + "?view=describe",
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
                "persistent_uri_path": bundle["persistent_uri"],
                "sparql_path": bundle["source_url"],
                "source_content_type": bundle["source_content_type"],
                "sparql_query_count": query_counter["total"],
            },
        )
        response.headers["Vary"] = "Accept"
        return response

    def _data_redirect_response(request: Request, resource_path: str, media_type: str, fmt: str) -> RedirectResponse:
        persistent_uri = f"{resolver.persistent_uri_base}{resource_path.strip('/')}"
        try:
            resolved = _fetch_doc_graph_from_datasets(
                backend=backend,
                resource_label=resource_path,
                persistent_uri=persistent_uri,
                dataset_names=_dataset_candidates(),
            )
            dataset_name = resolved[3] if resolved is not None else backend.default_dataset
        except RuntimeError:
            dataset_name = backend.default_dataset

        location = resolver.data_redirect_target(
            resource_label=resource_path,
            persistent_uri=persistent_uri,
            media_type=media_type,
            fmt=fmt,
            dataset=dataset_name,
        )

        response = RedirectResponse(url=location, status_code=303)
        response.headers["Vary"] = "Accept"
        return response

    @app.get("/doc/{resource_type}/{local_id}", name="get_doc_path", response_class=HTMLResponse)
    async def get_doc_path(request: Request, resource_type: str, local_id: str) -> HTMLResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        return _doc_render_response(request, _resource_path(resource_type, local_id))

    @app.get("/describe/{resource_type}/{local_id}", name="get_describe_path", response_class=HTMLResponse)
    async def get_describe_path(request: Request, resource_type: str, local_id: str) -> HTMLResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        return _describe_render_response(request, _resource_path(resource_type, local_id))

    @app.get("/graph/{resource_type}/{local_id}", name="get_graph_path", response_class=HTMLResponse)
    async def get_graph_path(request: Request, resource_type: str, local_id: str) -> HTMLResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found", "graph"}:
            raise HTTPException(status_code=404, detail="Not found")
        return _graph_render_response(request, _resource_path(resource_type, local_id))

    @app.get("/rdf/{resource_type}/{local_id}", name="get_rdf_path", response_class=HTMLResponse)
    async def get_rdf_path(
        request: Request,
        resource_type: str,
        local_id: str,
        view: str = "preview",
    ) -> HTMLResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found", "rdf"}:
            raise HTTPException(status_code=404, detail="Not found")
        normalized_view = "describe" if view == "describe" else "preview"
        return _rdf_render_response(request, _resource_path(resource_type, local_id), normalized_view)

    @app.get("/data/{resource_type}/{local_id}.{fmt}", name="get_data_format_path")
    async def get_data_format_path(
        request: Request,
        resource_type: str,
        local_id: str,
        fmt: str,
    ) -> RedirectResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        try:
            media_type = resolver.media_type_from_format(fmt)
        except UnsupportedFormatError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _data_redirect_response(
            request=request,
            resource_path=_resource_path(resource_type, local_id),
            media_type=media_type,
            fmt=fmt.lower(),
        )

    @app.get("/data/{resource_type}/{local_id}", name="get_data_path")
    async def get_data_path(request: Request, resource_type: str, local_id: str) -> RedirectResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        try:
            media_type = resolver.choose_data_media_type(request.headers.get("accept"))
        except NotAcceptableError as exc:
            raise HTTPException(status_code=406, detail=str(exc)) from exc

        return _data_redirect_response(
            request=request,
            resource_path=_resource_path(resource_type, local_id),
            media_type=media_type,
            fmt=MEDIA_TYPE_TO_FORMAT[media_type],
        )

    @app.get(
        "/not-found/{node_type}/{local_id}",
        name="get_not_found",
        response_class=HTMLResponse,
    )
    async def get_not_found(request: Request, node_type: NodeType, local_id: str) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="not_found.html",
            status_code=404,
            context={
                "node_type": node_type.value,
                "local_id": local_id,
                "css_path": _static_path_for_request(request, "not-found.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "dots_data_js_path": _static_path_for_request(request, "js/adam-dots-data.js"),
                "id_path": _public_path_for(
                    request,
                    "resolve_id",
                    node_type=node_type.value,
                    local_id=local_id,
                ),
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
            },
        )

    @app.get(
        "/not-found/{resource_type}/{local_id}",
        name="get_not_found_path",
        response_class=HTMLResponse,
    )
    async def get_not_found_path(request: Request, resource_type: str, local_id: str) -> HTMLResponse:
        if resource_type in {"doc", "data", "id", "resolve", "status", "not-found"}:
            raise HTTPException(status_code=404, detail="Not found")
        return templates.TemplateResponse(
            request=request,
            name="not_found.html",
            status_code=404,
            context={
                "node_type": resource_type,
                "local_id": local_id,
                "css_path": _static_path_for_request(request, "not-found.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "dots_data_js_path": _static_path_for_request(request, "js/adam-dots-data.js"),
                "id_path": _public_path_for(
                    request,
                    "resolve_generic_id",
                    resource_type=resource_type,
                    local_id=local_id,
                ),
                "home_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("root")),
                ),
            },
        )

    @app.get("/", name="root", response_class=HTMLResponse)
    async def root(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="home.html",
            context={
                "css_path": _static_path_for_request(request, "home.css"),
                "favicon_path": _static_path_for_request(request, "images/favicon.ico"),
                "favicon_png_path": _static_path_for_request(request, "images/favicon-32x32.png"),
                "apple_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-192x192.png",
                ),
                "chrome_touch_icon_path": _static_path_for_request(
                    request,
                    "images/android-chrome-512x512.png",
                ),
                "resolve_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("resolve_form")),
                ),
                "node_type_options": [
                    {"label": label, "value": node_type.value}
                    for label, node_type in NODE_TYPE_OPTIONS
                ],
                "default_node_type": NodeType.concept.value,
            },
        )

    @app.get("/status", name="status")
    async def status() -> dict[str, object]:
        return {
            "message": "URI resolver is running",
            "backend": "fuseki",
            "fuseki_server_url": app_settings.fuseki_server_url,
            "fuseki_dataset": app_settings.fuseki_dataset,
            "fuseki_datasets": _dataset_candidates(),
            "persistent_uri_base": app_settings.persistent_uri_base,
            "public_base_path": app_settings.public_base_path,
            "sample_id": _join_public_base_path(app_settings.public_base_path, "/id/concept/alice"),
        }

    return app


app = create_app()
