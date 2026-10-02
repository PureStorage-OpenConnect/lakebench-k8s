"""Kubernetes resource quantities: the one parser for capacity arithmetic.

Every place that adds up what pods request or what nodes offer (the
capacity preflight, the continuous stream budget, the autosizer, node
allocatable, the system fingerprint) reads quantities here, so a value
Kubernetes accepts is never misread or rejected by one of them.

The grammar is Kubernetes' own (``k8s.io/apimachinery`` resource.Quantity):
a number, then one of the binary suffixes ``Ki Mi Gi Ti Pi Ei``, the
decimal suffixes ``n u m k M G T P E``, an exponent (``1e3``, ``5E-2``) or
nothing. A suffix and an exponent never combine, and resource requests
are never negative.
"""

from __future__ import annotations

import math
import re
from decimal import ROUND_CEILING, Decimal, InvalidOperation

_FACTORS: dict[str, Decimal] = {
    "Ki": Decimal(1024),
    "Mi": Decimal(1024) ** 2,
    "Gi": Decimal(1024) ** 3,
    "Ti": Decimal(1024) ** 4,
    "Pi": Decimal(1024) ** 5,
    "Ei": Decimal(1024) ** 6,
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal(1),
    "k": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
}

_QUANTITY = re.compile(
    r"\s*(?P<num>\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
    r"(?:(?P<suffix>Ki|Mi|Gi|Ti|Pi|Ei|n|u|m|k|M|G|T|P|E)|[eE](?P<exp>[+-]?[0-9]+))?\s*"
)


class QuantityError(ValueError):
    """A value that is not a Kubernetes resource quantity."""


def parse(value: str | int | float) -> Decimal:
    """*value* as an exact Decimal in base units (bytes, or cores for CPU).

    Raises QuantityError for anything Kubernetes would reject, naming the
    value: lowercase ``g``, ``1.5gb``, a negative size, an empty string.
    """
    if isinstance(value, bool):
        raise QuantityError(f"{value!r} is not a Kubernetes quantity")
    if isinstance(value, (int, float)):
        if not math.isfinite(value) or value < 0:
            raise QuantityError(f"{value!r} is not a resource quantity")
        return Decimal(str(value))
    m = _QUANTITY.fullmatch(str(value))
    if not m:
        raise QuantityError(
            f"{value!r} is not a Kubernetes quantity (for example 16Gi, 4G, 500m, 2 or 1e3)"
        )
    try:
        number = Decimal(m.group("num"))
    except InvalidOperation as e:  # pragma: no cover -- the pattern admits only digits
        raise QuantityError(f"{value!r} is not a Kubernetes quantity") from e
    if m.group("exp") is not None:
        return number * (Decimal(10) ** int(m.group("exp")))
    return number * _FACTORS[m.group("suffix") or ""]


def to_bytes(value: str | int | float) -> int:
    """Whole bytes, rounded up (Kubernetes rounds a fractional request up)."""
    return int(parse(value).to_integral_value(rounding=ROUND_CEILING))


def to_millicores(value: str | int | float) -> int:
    """Whole millicores, rounded up."""
    return int((parse(value) * 1000).to_integral_value(rounding=ROUND_CEILING))


def to_gib(value: str | int | float) -> float:
    """GiB as a float."""
    return float(parse(value) / _FACTORS["Gi"])
