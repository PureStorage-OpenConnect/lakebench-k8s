"""Did-you-mean hints for config errors.

An unknown key is matched first against the keys its own section accepts,
then against the last part of every key path in the schema, so a key written
in the wrong section is pointed at the section it belongs to. Paths are the
ones a user writes: ``workload`` at the top level, ``pipeline.continuous``
and ``workload.schema``, not where the model stores them.
"""

from __future__ import annotations

import difflib
import typing
from functools import lru_cache
from typing import Any

from pydantic import BaseModel

CUTOFF = 0.75
# The whole-schema pass looks for a key written in the wrong section, so it
# asks for a closer match: at 0.75, 'schema_version' would be pointed at
# 'observability.chart_version'.
ELSEWHERE_CUTOFF = 0.85

# Keys load_config promotes from the top level (loader._FLAT_FIELD_MAP),
# kept in step by test_flat_keys_are_known_top_level_keys.
FLAT_KEYS = (
    "endpoint",
    "access_key",
    "secret_key",
    "secret_ref",
    "scale",
    "namespace",
    "mode",
    "cycles",
    "spark_image",
)

# Field name -> the key the user writes, per model, where they differ.
_SPELLING = {
    ("ProcessingConfig", "sustained"): "continuous",
}


def _models_in(annotation: object) -> list[type[BaseModel]]:
    """The config models an annotation holds (through Optional, list, dict)."""
    found: list[type[BaseModel]] = []
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    for arg in typing.get_args(annotation):
        found.extend(_models_in(arg))
    return found


def _user_key(model: type[BaseModel], name: str) -> str:
    field = model.model_fields[name]
    return _SPELLING.get((model.__name__, name)) or field.alias or name


def section_keys(model: type[BaseModel]) -> dict[str, str]:
    """Key the user writes -> field name, for one section."""
    keys = {_user_key(model, n): n for n in model.model_fields}
    if model.__name__ == "ArchitectureConfig":
        keys.pop("workload", None)  # written at the top level
    if model.__name__ == "LakebenchConfig":
        keys["workload"] = "workload"
        for flat in FLAT_KEYS:
            keys[flat] = flat
    return keys


def _root() -> type[BaseModel]:
    from lakebench.config.schema import LakebenchConfig

    return LakebenchConfig


@lru_cache(maxsize=1)
def all_paths() -> tuple[str, ...]:
    """Every key path a user can write (sections and leaves), dotted."""
    root = _root()
    paths: list[str] = []

    def walk(model: type[BaseModel], prefix: str, depth: int) -> None:
        for key, name in section_keys(model).items():
            if model is root and key in FLAT_KEYS:
                continue
            if model is root and key == "workload":
                sub = _models_in(root.model_fields["architecture"].annotation)
                target = sub[0].model_fields["workload"].annotation if sub else None
            else:
                target = model.model_fields[name].annotation
            path = f"{prefix}{key}"
            paths.append(path)
            if depth < 8:
                for sub_model in _models_in(target):
                    walk(sub_model, path + ".", depth + 1)

    walk(root, "", 0)
    return tuple(dict.fromkeys(paths))


def model_at(loc: tuple[Any, ...]) -> type[BaseModel] | None:
    """The model a validation-error location (model terms) points into."""
    model: type[BaseModel] = _root()
    for part in loc:
        if isinstance(part, int):
            continue  # a list index stays inside the same item model
        field_name = None
        if part in model.model_fields:
            field_name = part
        else:
            for name, field in model.model_fields.items():
                if field.alias == part:
                    field_name = name
        if field_name is None:
            return None
        subs = _models_in(model.model_fields[field_name].annotation)
        if not subs:
            return None
        model = subs[0]
    return model


def unknown_key_hint(loc: tuple[Any, ...], user_loc: tuple[Any, ...]) -> str | None:
    """``did you mean ...`` for an unknown key at model location *loc*.

    *user_loc* is the same location as the user wrote it, used to prefer the
    candidate path nearest to where the key was written.
    """
    if not loc:
        return None
    key = str(loc[-1])
    parent = model_at(loc[:-1])
    if parent is not None:
        siblings = sorted(section_keys(parent))
        near = difflib.get_close_matches(key, siblings, n=1, cutoff=CUTOFF)
        if near:
            # A flat spelling is accepted but deprecated: offer the nested key.
            if parent is _root() and near[0] in FLAT_KEYS:
                from lakebench.config.loader import _FLAT_FIELD_MAP

                return f"did you mean `{'.'.join(_FLAT_FIELD_MAP[near[0]])}`?"
            return f"did you mean `{near[0]}`?"
    by_leaf: dict[str, list[str]] = {}
    for path in all_paths():
        by_leaf.setdefault(path.rsplit(".", 1)[-1], []).append(path)
    near = difflib.get_close_matches(key, sorted(by_leaf), n=1, cutoff=ELSEWHERE_CUTOFF)
    if not near:
        return None
    written = ".".join(str(p) for p in user_loc[:-1] if not isinstance(p, int))

    def shared_prefix(path: str) -> int:
        return len(_common_prefix(path.rsplit(".", 1)[0], written))

    best = sorted(by_leaf[near[0]], key=lambda p: (-shared_prefix(p), len(p), p))[0]
    return f"did you mean `{best}`?"


def _common_prefix(a: str, b: str) -> str:
    parts_a, parts_b = a.split("."), b.split(".")
    common = []
    for x, y in zip(parts_a, parts_b, strict=False):
        if x != y:
            break
        common.append(x)
    return ".".join(common)


def nearest(name: str, candidates: typing.Iterable[str]) -> str | None:
    found = difflib.get_close_matches(name, sorted(candidates), n=1, cutoff=CUTOFF)
    return found[0] if found else None
