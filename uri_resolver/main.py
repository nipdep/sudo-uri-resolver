from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request as URLRequest
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .backend import FusekiRedirectBackend, SPARQL_JSON
from .models import NodeType
from .services import (
    MEDIA_TYPE_TO_FORMAT,
    NotAcceptableError,
    ResolverService,
    UnsupportedFormatError,
)
from .settings import AppSettings

logger = logging.getLogger("uri_resolver.doc")

PREFIX_MAP = (
    ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"),
    ("http://www.w3.org/2000/01/rdf-schema#", "rdfs:"),
    ("http://www.w3.org/2002/07/owl#", "owl:"),
    ("http://www.w3.org/2004/02/skos/core#", "skos:"),
    ("http://www.w3.org/ns/prov#", "prov:"),
    ("http://schema.org/", "schema:"),
    ("http://purl.org/dc/terms/", "dcterms:"),
    ("http://www.wikidata.org/prop/direct/", "wdt:"),
)

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

NODE_TYPE_OPTIONS = (
    ("Author", NodeType.author),
    ("Paper", NodeType.paper),
    ("Concept", NodeType.concept),
    ("Artifact", NodeType.artifact),
    ("Proposition", NodeType.proposition),
)


def _join_public_base_path(public_base_path: str, path: str) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    if not public_base_path:
        return normalized_path
    return f"{public_base_path}{normalized_path}"


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
    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return normalized.strip("-")


def fetch_doc_statements(
    url: str,
    timeout: float = 10.0,
) -> tuple[dict[str, object], str]:
    """Fetch SPARQL JSON statements from Fuseki for HTML rendering."""
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
            content_type = response.headers.get("Content-Type", SPARQL_JSON)
            charset = response.headers.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            return json.loads(text), content_type
    except HTTPError as exc:
        message = f"Fuseki returned HTTP {exc.code}"
        if exc.fp:
            detail = exc.read().decode("utf-8", errors="replace")
            message = f"{message}: {detail[:300]}"
        raise RuntimeError(message) from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Fuseki: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Fuseki response was not valid JSON: {exc}") from exc


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


def _parse_statement_value(value: dict[str, str]) -> dict[str, str | bool | None]:
    value_type = value.get("type", "literal")
    raw_value = value.get("value", "")

    if value_type == "uri":
        return {
            "is_uri": True,
            "uri": raw_value,
            "display": _compact_uri(raw_value),
            "meta": None,
        }

    if value_type == "bnode":
        return {
            "is_uri": False,
            "uri": None,
            "display": f"_:{raw_value}",
            "meta": None,
        }

    language = value.get("xml:lang")
    datatype = value.get("datatype")
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
    payload: dict[str, object],
    node_type: NodeType,
    local_id: str,
    persistent_uri: str,
) -> dict[str, object]:
    bindings = payload.get("results", {})
    if isinstance(bindings, dict):
        raw_bindings = bindings.get("bindings", [])
    else:
        raw_bindings = []

    statements_by_predicate: dict[str, dict[str, object]] = {}
    title: str | None = None
    description: str | None = None

    if isinstance(raw_bindings, list):
        for row in raw_bindings:
            if not isinstance(row, dict):
                continue

            pred_obj = row.get("p")
            obj_obj = row.get("o")
            if not isinstance(pred_obj, dict) or not isinstance(obj_obj, dict):
                continue

            predicate_uri = pred_obj.get("value")
            if not isinstance(predicate_uri, str) or not predicate_uri:
                continue

            parsed_value = _parse_statement_value(obj_obj)
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

            if (
                description is None
                and predicate_uri in DESCRIPTION_PREDICATES
                and not parsed_value["is_uri"]
            ):
                candidate = parsed_value["display"]
                if isinstance(candidate, str) and candidate.strip():
                    description = candidate.strip()

    sorted_statements = sorted(
        statements_by_predicate.values(),
        key=lambda item: str(item.get("predicate_label", "")),
    )

    return {
        "title": title or f"{node_type.value}/{local_id}",
        "description": description,
        "node_type": node_type.value,
        "local_id": local_id,
        "persistent_uri": persistent_uri,
        "statement_count": sum(
            len(item.get("values", [])) if isinstance(item.get("values"), list) else 0
            for item in sorted_statements
        ),
        "statements": sorted_statements,
    }


def _is_empty_doc_payload(payload: dict[str, object]) -> bool:
    results = payload.get("results", {})
    if not isinstance(results, dict):
        return True
    bindings = results.get("bindings", [])
    return not isinstance(bindings, list) or len(bindings) == 0


def create_app(settings: AppSettings | None = None) -> FastAPI:
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
    app.mount("/static", StaticFiles(directory=str(base_dir / "static")), name="static")

    logger.info(
        "resolver_config public_base_path=%s",
        app_settings.public_base_path or "<root>",
    )

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

    @app.get("/id/{node_type}/{local_id}", name="resolve_id")
    async def resolve_id(request: Request, node_type: NodeType, local_id: str) -> RedirectResponse:
        try:
            choice = resolver.choose_id_representation(request.headers.get("accept"))
        except NotAcceptableError as exc:
            raise HTTPException(status_code=406, detail=str(exc)) from exc

        if choice.kind == "doc":
            location = _public_path_for(
                request,
                "get_doc",
                node_type=node_type.value,
                local_id=local_id,
            )
        else:
            location = _public_path_for(
                request,
                "get_data_format",
                node_type=node_type.value,
                local_id=local_id,
                fmt=choice.fmt,
            )

        response = RedirectResponse(url=location, status_code=303)
        response.headers["Vary"] = "Accept"
        return response

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

    @app.get("/doc/{node_type}/{local_id}", name="get_doc", response_class=HTMLResponse)
    async def get_doc(request: Request, node_type: NodeType, local_id: str) -> HTMLResponse:
        identifier = resolver.to_identifier(node_type, local_id)
        persistent_uri = resolver.build_persistent_uri(identifier)

        source_url = resolver.doc_redirect_target(identifier, persistent_uri)

        try:
            payload, source_content_type = fetch_doc_statements(source_url)
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        if _is_empty_doc_payload(payload):
            logger.info(
                "entity_not_found resource=%s/%s persistent_uri=%s",
                node_type.value,
                local_id,
                persistent_uri,
            )
            not_found_path = _public_path_for(
                request,
                "get_not_found",
                node_type=node_type.value,
                local_id=local_id,
            )
            return RedirectResponse(url=not_found_path, status_code=303)

        logger.info(
            "doc_rendered resource=%s/%s persistent_uri=%s source_url=%s",
            node_type.value,
            local_id,
            persistent_uri,
            source_url,
        )

        view = build_doc_view(
            payload=payload,
            node_type=node_type,
            local_id=local_id,
            persistent_uri=persistent_uri,
        )
        return templates.TemplateResponse(
            request=request,
            name="resource.html",
            context={
                **view,
                "css_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("static", path="doc.css")),
                ),
                "doc_path": _public_path_for(
                    request,
                    "get_doc",
                    node_type=node_type.value,
                    local_id=local_id,
                ),
                "data_path": _public_path_for(
                    request,
                    "get_data",
                    node_type=node_type.value,
                    local_id=local_id,
                ),
                "data_ttl_path": _public_path_for(
                    request,
                    "get_data_format",
                    node_type=node_type.value,
                    local_id=local_id,
                    fmt="ttl",
                ),
                "data_jsonld_path": _public_path_for(
                    request,
                    "get_data_format",
                    node_type=node_type.value,
                    local_id=local_id,
                    fmt="jsonld",
                ),
                "data_rdf_path": _public_path_for(
                    request,
                    "get_data_format",
                    node_type=node_type.value,
                    local_id=local_id,
                    fmt="rdf",
                ),
                "source_url": source_url,
                "source_content_type": source_content_type,
            },
        )

    @app.get("/data/{node_type}/{local_id}.{fmt}", name="get_data_format")
    async def get_data_format(
        node_type: NodeType,
        local_id: str,
        fmt: str,
    ) -> RedirectResponse:
        identifier = resolver.to_identifier(node_type, local_id)

        try:
            media_type = resolver.media_type_from_format(fmt)
        except UnsupportedFormatError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        persistent_uri = resolver.build_persistent_uri(identifier)
        location = resolver.data_redirect_target(
            identifier=identifier,
            persistent_uri=persistent_uri,
            media_type=media_type,
            fmt=fmt.lower(),
        )

        response = RedirectResponse(url=location, status_code=303)
        response.headers["Vary"] = "Accept"
        return response

    @app.get("/data/{node_type}/{local_id}", name="get_data")
    async def get_data(request: Request, node_type: NodeType, local_id: str) -> RedirectResponse:
        identifier = resolver.to_identifier(node_type, local_id)

        try:
            media_type = resolver.choose_data_media_type(request.headers.get("accept"))
        except NotAcceptableError as exc:
            raise HTTPException(status_code=406, detail=str(exc)) from exc

        persistent_uri = resolver.build_persistent_uri(identifier)
        location = resolver.data_redirect_target(
            identifier=identifier,
            persistent_uri=persistent_uri,
            media_type=media_type,
            fmt=MEDIA_TYPE_TO_FORMAT[media_type],
        )

        response = RedirectResponse(url=location, status_code=303)
        response.headers["Vary"] = "Accept"
        return response

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
                "css_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("static", path="not-found.css")),
                ),
                "dots_data_js_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("static", path="js/adam-dots-data.js")),
                ),
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

    @app.get("/", name="root", response_class=HTMLResponse)
    async def root(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request=request,
            name="home.html",
            context={
                "css_path": _join_public_base_path(
                    _public_base_path_for_request(request),
                    str(app.url_path_for("static", path="home.css")),
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
    async def status() -> dict[str, str]:
        return {
            "message": "URI resolver is running",
            "backend": "fuseki",
            "fuseki_server_url": app_settings.fuseki_server_url,
            "fuseki_dataset": app_settings.fuseki_dataset,
            "persistent_uri_base": app_settings.persistent_uri_base,
            "public_base_path": app_settings.public_base_path,
            "sample_id": _join_public_base_path(app_settings.public_base_path, "/id/concept/alice"),
        }

    return app


app = create_app()
