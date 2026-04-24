from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from .backend import JSONLD, RDFXML, TURTLE, FusekiRedirectBackend
from .models import NodeType, ResourceIdentifier

HTML = "text/html"

FORMAT_TO_MEDIA_TYPE = {
    "jsonld": JSONLD,
    "ttl": TURTLE,
    "rdf": RDFXML,
}

MEDIA_TYPE_TO_FORMAT = {value: key for key, value in FORMAT_TO_MEDIA_TYPE.items()}


class ResolverError(Exception):
    """Base domain error for resolver services."""


class UnsupportedFormatError(ResolverError):
    """Raised when an explicit format suffix is unknown."""


class NotAcceptableError(ResolverError):
    """Raised when content negotiation cannot pick a representation."""


@dataclass(frozen=True)
class RepresentationChoice:
    media_type: str
    kind: str  # "doc" | "data"
    fmt: str | None = None


class ContentNegotiator:
    """Small RFC7231-style Accept negotiator with q-value support."""

    def select(self, accept_header: str | None, supported: list[str]) -> str | None:
        parsed = self._parse_accept(accept_header)
        best_media: str | None = None
        best_score = (-1.0, -1, -1, -1)

        for supported_index, media_type in enumerate(supported):
            match = self._best_match_for(media_type, parsed)
            if match is None:
                continue

            quality, specificity, neg_order = match
            score = (quality, specificity, -neg_order, -supported_index)
            if score > best_score:
                best_score = score
                best_media = media_type

        return best_media

    def _parse_accept(self, accept_header: str | None) -> list[tuple[str, float, int]]:
        if accept_header is None or not accept_header.strip():
            return [("*/*", 1.0, 0)]

        parsed: list[tuple[str, float, int]] = []
        for order, token in enumerate(accept_header.split(",")):
            item = token.strip().lower()
            if not item:
                continue

            media_range = item
            q_value = 1.0
            if ";" in item:
                parts = [part.strip() for part in item.split(";") if part.strip()]
                media_range = parts[0]
                for param in parts[1:]:
                    if "=" not in param:
                        continue
                    key, value = param.split("=", 1)
                    if key.strip() == "q":
                        try:
                            q_value = float(value.strip())
                        except ValueError:
                            q_value = 0.0

            q_value = max(0.0, min(q_value, 1.0))
            parsed.append((media_range, q_value, order))

        return parsed or [("*/*", 1.0, 0)]

    def _best_match_for(
        self, media_type: str, parsed_accept: list[tuple[str, float, int]]
    ) -> tuple[float, int, int] | None:
        if "/" not in media_type:
            return None

        candidate_type, candidate_subtype = media_type.split("/", 1)
        best: tuple[float, int, int] | None = None

        for media_range, quality, order in parsed_accept:
            if quality <= 0.0:
                continue
            if "/" not in media_range:
                continue

            accepted_type, accepted_subtype = media_range.split("/", 1)
            specificity = -1
            if accepted_type == "*" and accepted_subtype == "*":
                specificity = 0
            elif accepted_type == candidate_type and accepted_subtype == "*":
                specificity = 1
            elif accepted_type == candidate_type and accepted_subtype == candidate_subtype:
                specificity = 2

            if specificity < 0:
                continue

            score = (quality, specificity, -order)
            if best is None or score > (best[0], best[1], -best[2]):
                best = (quality, specificity, order)

        return best


class ResolverService:
    """Fuseki-focused service layer used by routing."""

    def __init__(self, backend: FusekiRedirectBackend, persistent_uri_base: str) -> None:
        self.backend = backend
        self.negotiator = ContentNegotiator()
        self.persistent_uri_base = self._normalize_base(persistent_uri_base)

    def to_identifier(self, node_type: NodeType, local_id: str) -> ResourceIdentifier:
        return ResourceIdentifier(node_type=node_type, local_id=local_id)

    def build_persistent_uri(self, identifier: ResourceIdentifier) -> str:
        encoded_local_id = quote(identifier.local_id, safe="")
        return f"{self.persistent_uri_base}{identifier.node_type.value}/{encoded_local_id}"

    def doc_redirect_target(self, identifier: ResourceIdentifier, persistent_uri: str) -> str:
        return self.backend.get_doc_target(identifier, persistent_uri)

    def data_redirect_target(
        self,
        identifier: ResourceIdentifier,
        persistent_uri: str,
        media_type: str,
        fmt: str | None = None,
    ) -> str:
        return self.backend.get_data_target(identifier, persistent_uri, media_type, fmt=fmt)

    def choose_id_representation(self, accept_header: str | None) -> RepresentationChoice:
        supported = [HTML, JSONLD, TURTLE, RDFXML]
        selected = self.negotiator.select(accept_header, supported)
        if selected is None:
            raise NotAcceptableError("No acceptable representation for /id endpoint")

        if selected == HTML:
            return RepresentationChoice(media_type=HTML, kind="doc")

        return RepresentationChoice(
            media_type=selected,
            kind="data",
            fmt=MEDIA_TYPE_TO_FORMAT[selected],
        )

    def choose_data_media_type(self, accept_header: str | None) -> str:
        supported = [JSONLD, TURTLE, RDFXML]
        selected = self.negotiator.select(accept_header, supported)
        if selected is None:
            raise NotAcceptableError("No acceptable machine-readable representation")
        return selected

    def media_type_from_format(self, fmt: str) -> str:
        media_type = FORMAT_TO_MEDIA_TYPE.get(fmt.lower())
        if media_type is None:
            raise UnsupportedFormatError(f"Unknown format: {fmt}")
        return media_type

    @staticmethod
    def _normalize_base(value: str) -> str:
        return value.rstrip("/") + "/"
