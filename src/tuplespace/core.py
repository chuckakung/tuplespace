"""
Core types for TupleSpace: WILDCARD, Template, TupleEntry.

JSON wire protocol sentinels:
    WILDCARD  -> "__WILDCARD__"
    int       -> "__TYPE:int__"
    str       -> "__TYPE:str__"
    float     -> "__TYPE:float__"
    bool      -> "__TYPE:bool__"
    list      -> "__TYPE:list__"
    dict      -> "__TYPE:dict__"
"""

import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Union


class Wildcard:
    """Represents a wildcard that matches any value in a template."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "WILDCARD"


WILDCARD = Wildcard()

# --- JSON wire protocol helpers ---

_WILDCARD_SENTINEL = "__WILDCARD__"
_TYPE_PREFIX = "__TYPE:"
_TYPE_SUFFIX = "__"

_SUPPORTED_TYPES = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
}
_TYPE_NAMES = {v: k for k, v in _SUPPORTED_TYPES.items()}


def encode_template(template: Sequence) -> list:
    """Encode a template (with WILDCARD / type objects) into JSON-safe list."""
    result = []
    for item in template:
        if isinstance(item, Wildcard):
            result.append(_WILDCARD_SENTINEL)
        elif isinstance(item, type) and item in _TYPE_NAMES:
            result.append(f"{_TYPE_PREFIX}{_TYPE_NAMES[item]}{_TYPE_SUFFIX}")
        else:
            result.append(item)
    return result


def decode_template(data: list) -> tuple:
    """Decode a JSON template list back to a tuple with WILDCARD / type objects."""
    result = []
    for item in data:
        if item == _WILDCARD_SENTINEL:
            result.append(WILDCARD)
        elif (
            isinstance(item, str)
            and item.startswith(_TYPE_PREFIX)
            and item.endswith(_TYPE_SUFFIX)
        ):
            type_name = item[len(_TYPE_PREFIX) : -len(_TYPE_SUFFIX)]
            if type_name in _SUPPORTED_TYPES:
                result.append(_SUPPORTED_TYPES[type_name])
            else:
                result.append(item)
        else:
            result.append(item)
    return tuple(result)


def result_to_tuple(data: Any) -> Optional[tuple]:
    """Convert a JSON result (list) back to a Python tuple."""
    if data is None:
        return None
    return tuple(data)


@dataclass
class TupleEntry:
    """Internal representation of a tuple with metadata."""

    data: Union[tuple, list]
    expire_time: Optional[float] = None
    entry_id: int = 0

    def is_expired(self) -> bool:
        """Check if this tuple has expired."""
        if self.expire_time is None:
            return False
        return time.time() > self.expire_time


class Template:
    """Template for pattern matching tuples."""

    def __init__(self, pattern: Union[tuple, list]):
        self.pattern = pattern

    def matches(self, tuple_data: Union[tuple, list]) -> bool:
        """Check if a tuple matches this template."""
        if len(self.pattern) != len(tuple_data):
            return False

        for template_item, tuple_item in zip(self.pattern, tuple_data):
            if isinstance(template_item, Wildcard):
                continue
            elif isinstance(template_item, type):
                if not isinstance(tuple_item, template_item):
                    return False
            elif template_item != tuple_item:
                return False

        return True

    def __repr__(self):
        return f"Template({self.pattern!r})"
