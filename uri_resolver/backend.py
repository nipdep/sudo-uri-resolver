from __future__ import annotations

import logging
from urllib.parse import quote, urlsplit, urlunsplit

JSONLD = "application/ld+json"
TURTLE = "text/turtle"
RDFXML = "application/rdf+xml"
SPARQL_JSON = "application/sparql-results+json"
NQUADS = "application/n-quads"

logger = logging.getLogger("uri_resolver.fuseki")


class FusekiRedirectBackend:
    """Builds Apache Jena Fuseki redirect targets for URI representations."""

    def __init__(self, server_url: str, dataset: str = "gold_standard_kg") -> None:
        self.server_url = self._normalize_server_url(server_url)
        self.default_dataset = self._normalize_dataset(dataset)

    def get_doc_target(
        self,
        resource_label: str,
        persistent_uri: str,
        dataset: str | None = None,
    ) -> str:
        doc_query = self._describe_query(persistent_uri)
        return self._query_target(
            resource_label=resource_label,
            query=doc_query,
            output=TURTLE,
            dataset=dataset,
            log_label="fuseki_doc_query",
        )

    def get_data_target(
        self,
        resource_label: str,
        persistent_uri: str,
        media_type: str,
        fmt: str | None = None,
        dataset: str | None = None,
    ) -> str:
        describe_query = self._describe_query(persistent_uri)
        output = self._fuseki_output_hint(media_type=media_type, fmt=fmt)
        return self._query_target(
            resource_label=resource_label,
            query=describe_query,
            output=output,
            dataset=dataset,
            log_label="fuseki_data_redirect",
        )

    def get_select_target(
        self,
        resource_label: str,
        query: str,
        dataset: str | None = None,
    ) -> str:
        return self._query_target(
            resource_label=resource_label,
            query=query,
            output=SPARQL_JSON,
            dataset=dataset,
            log_label="fuseki_select_query",
        )

    def get_nquads_target(
        self,
        resource_label: str,
        query: str,
        dataset: str | None = None,
    ) -> str:
        return self._query_target(
            resource_label=resource_label,
            query=query,
            output=NQUADS,
            dataset=dataset,
            log_label="fuseki_nquads_query",
        )

    @staticmethod
    def _normalize_server_url(server_url: str) -> str:
        parsed = urlsplit(server_url)
        normalized = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        return normalized.rstrip("/")

    @staticmethod
    def _normalize_dataset(dataset: str) -> str:
        return dataset.strip().strip("/")

    def _query_target(
        self,
        resource_label: str,
        query: str,
        output: str,
        dataset: str | None,
        log_label: str,
    ) -> str:
        dataset_name = self._normalize_dataset(dataset or self.default_dataset)
        target = (
            f"{self.server_url}/{dataset_name}/query"
            f"?query={quote(query, safe='')}&output={quote(output, safe='')}"
        )
        logger.info(
            "%s resource=%s dataset=%s query=%s output=%s target=%s",
            log_label,
            resource_label,
            dataset_name,
            query,
            output,
            target,
        )
        return target

    @staticmethod
    def _describe_query(persistent_uri: str) -> str:
        return f"DESCRIBE <{persistent_uri}>"

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
