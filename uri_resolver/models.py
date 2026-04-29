from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class NodeType(StrEnum):
    """Allowed KG node types in persistent URI paths."""

    author = "author"
    paper = "paper"
    concept = "concept"
    proposition = "proposition"


class ResourceIdentifier(BaseModel):
    """Canonical ID for a resource in the resolver namespace."""

    node_type: NodeType
    local_id: str = Field(min_length=1)
