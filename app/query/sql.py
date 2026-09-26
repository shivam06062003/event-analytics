"""SQL building blocks. THE RULE: user input never becomes SQL text.

Every value, including property NAMES, travels as a server-side query
parameter ({name:Type}). ClickHouse parses the SQL first and binds the values
afterwards, so a value can never change the query's structure. The only
things interpolated into SQL text are identifiers and operators chosen from
fixed allow-lists in this module.
"""

from datetime import UTC, datetime
from typing import Any

from app.schemas.queries import PropertyFilter

_NUMERIC_OPS = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}


class Params:
    """Collects bound parameters and hands out unique placeholder names."""

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def add(self, value: Any, type_: str) -> str:
        name = f"p{len(self.values)}"
        self.values[name] = value
        return f"{{{name}:{type_}}}"


def ch_datetime(value: datetime) -> str:
    """ClickHouse DateTime64 literal in UTC."""
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def property_value_sql(name_placeholder: str) -> str:
    """A property as a display string: strings unquoted, numbers/bools as raw JSON."""
    return (
        f"if(JSONType(properties, {name_placeholder}) = 'String', "
        f"JSONExtractString(properties, {name_placeholder}), "
        f"JSONExtractRaw(properties, {name_placeholder}))"
    )


def filter_sql(f: PropertyFilter, params: Params) -> str:
    """Comparisons only match events that HAVE the property.

    JSONExtractFloat/String return 0 / '' for a missing key, so without the
    JSONHas guard "value <= 50" would match every event with no value at all.
    (Same as SQL NULL semantics; use is_not_set to find missing properties.)
    """
    key = params.add(f.property, "String")
    if f.operator == "is_set":
        return f"JSONHas(properties, {key})"
    if f.operator == "is_not_set":
        return f"NOT JSONHas(properties, {key})"
    return f"(JSONHas(properties, {key}) AND {_comparison_sql(f, key, params)})"


def _comparison_sql(f: PropertyFilter, key: str, params: Params) -> str:
    if f.operator in _NUMERIC_OPS:
        value = params.add(float(f.value), "Float64")  # type: ignore[arg-type]
        return f"JSONExtractFloat(properties, {key}) {_NUMERIC_OPS[f.operator]} {value}"
    if f.operator == "contains":
        value = params.add(f.value, "String")
        return f"positionCaseInsensitive(JSONExtractString(properties, {key}), {value}) > 0"
    # eq / neq: compare with the JSON type the value actually has.
    symbol = "=" if f.operator == "eq" else "!="
    if isinstance(f.value, bool):
        return f"JSONExtractBool(properties, {key}) {symbol} {params.add(f.value, 'Bool')}"
    if isinstance(f.value, int | float):
        return (
            f"JSONExtractFloat(properties, {key}) {symbol} {params.add(float(f.value), 'Float64')}"
        )
    return f"JSONExtractString(properties, {key}) {symbol} {params.add(f.value, 'String')}"


def filters_sql(filters: list[PropertyFilter], params: Params) -> str:
    return "".join(f" AND {filter_sql(f, params)}" for f in filters)


def base_where(project_id: str, start: datetime, end: datetime, params: Params) -> str:
    """Leading predicates match the table's sort key (project_id, date, event),
    so ClickHouse skips every granule outside this project and time range."""
    return (
        f"project_id = {params.add(project_id, 'UUID')} "
        f"AND timestamp >= {params.add(ch_datetime(start), 'DateTime64(3)')} "
        f"AND timestamp < {params.add(ch_datetime(end), 'DateTime64(3)')}"
    )
