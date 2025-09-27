from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, List

TPCH_TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]

TPCH_QUERY_NUMBERS = tuple(range(1, 23))


@dataclass(frozen=True)
class ScaleInfo:
    """Represents a single TPC-H scale factor."""

    original: str
    normalized: str
    slug: str

    @property
    def dataset_name(self) -> str:
        return f"tpch_sf{self.slug}"

    @property
    def result_dir(self) -> str:
        return f"sf{self.normalized}"

    @property
    def group_tag(self) -> str:
        return f"sf{self.slug}"

    @property
    def display_name(self) -> str:
        return f"SF{self.normalized}"


def _normalise_scale_string(value: str) -> str:
    return value.replace("_", ".")


def parse_scale(value: str) -> ScaleInfo:
    raw = str(value).strip()
    normalised_input = _normalise_scale_string(raw)
    try:
        numeric = Decimal(normalised_input)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid scale factor '{value}'") from exc
    if numeric < 0:
        raise ValueError(f"Scale factor must be non-negative, received '{value}'")
    normalised = format(numeric.normalize(), "f")
    slug = normalised.replace(".", "_")
    return ScaleInfo(original=raw, normalized=normalised, slug=slug)


def parse_scale_list(values: Iterable[str] | None, default: Iterable[str] | None = None) -> List[ScaleInfo]:
    value_list = list(values or [])
    if not value_list and default is not None:
        value_list = list(default)
    if not value_list:
        raise ValueError("At least one scale factor must be provided")
    return [parse_scale(value) for value in value_list]
