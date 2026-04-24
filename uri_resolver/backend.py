from __future__ import annotations

import logging
from urllib.parse import quote, urlsplit, urlunsplit

from .models import ResourceIdentifier

JSONLD = "application/ld+json"
TURTLE = "text/turtle"
RDFXML = "application/rdf+xml"
SPARQL_JSON = "application/sparql-results+json"

logger = logging.getLogger("uri_resolver.fuseki")


class FusekiRedirectBackend:
    """Builds Apache Jena Fuseki redirect targets for URI representations."""

    def __init__(self, server_url: str, dataset: str = "idea_kg") -> None:
        self.server_url = self._normalize_server_url(server_url)
        self.dataset = self._normalize_dataset(dataset)

    def get_doc_target(self, identifier: ResourceIdentifier, persistent_uri: str) -> str:
        doc_query = self._doc_query(persistent_uri)
        target = (
            f"{self.server_url}/{self.dataset}/query"
            f"?query={quote(doc_query, safe='')}&output={quote(SPARQL_JSON, safe='')}"
        )
        logger.info(
            "fuseki_doc_query resource=%s/%s query=%s output=%s target=%s",
            identifier.node_type.value,
            identifier.local_id,
            doc_query,
            SPARQL_JSON,
            target,
        )
        return target

    def get_data_target(
        self,
        identifier: ResourceIdentifier,
        persistent_uri: str,
        media_type: str,
        fmt: str | None = None,
    ) -> str:
        describe_query = self._describe_query(persistent_uri)
        output = self._fuseki_output_hint(media_type=media_type, fmt=fmt)
        target = (
            f"{self.server_url}/{self.dataset}/query"
            f"?query={quote(describe_query, safe='')}&output={quote(output, safe='')}"
        )
        logger.info(
            "fuseki_data_redirect resource=%s/%s query=%s output=%s target=%s",
            identifier.node_type.value,
            identifier.local_id,
            describe_query,
            output,
            target,
        )
        return target

    @staticmethod
    def _normalize_server_url(server_url: str) -> str:
        parsed = urlsplit(server_url)
        normalized = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        return normalized.rstrip("/")

    @staticmethod
    def _normalize_dataset(dataset: str) -> str:
        return dataset.strip().strip("/")

    @staticmethod
    def _describe_query(persistent_uri: str) -> str:
        return f"DESCRIBE <{persistent_uri}>"

    @staticmethod
    def _doc_query(persistent_uri: str) -> str:
        return f"SELECT ?p ?o WHERE {{ <{persistent_uri}> ?p ?o }} ORDER BY STR(?p) STR(?o)"

    @staticmethod
    def _fuseki_output_hint(media_type: str, fmt: str | None) -> str:
        if fmt:
            fmt_key = fmt.lower()
            if fmt_key == "ttl":
                return TURTLE
            if fmt_key == "rdf":
                return RDFXML
            if fmt_key == "jsonld":
                return JSONLD
        return media_type
