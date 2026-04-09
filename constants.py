# ─────────────────────────────────────────────
# types.py  —  constants, type maps, role helpers
# ─────────────────────────────────────────────

TYPE_MAP = {
    "number":   "double",
    "integer":  "int64",
    "text":     "string",
    "string":   "string",
    "boolean":  "boolean",
    "datetime": "dateTime",
    "date":     "dateTime",
    "currency": "double",
    "decimal":  "double",
    "int":      "int64",
    "float":    "double",
}

PQUERY_TYPE_MAP = {
    "number":   "type number",
    "integer":  "Int64.Type",
    "text":     "type text",
    "string":   "type text",
    "boolean":  "type logical",
    "datetime": "type datetime",
    "date":     "type date",
    "currency": "Currency.Type",
    "decimal":  "type number",
    "int":      "Int64.Type",
    "float":    "type number",
}

VISUAL_TYPE_MAP = {
    "barChart":     "barChart",
    "columnChart":  "columnChart",
    "lineChart":    "lineChart",
    "donutChart":   "donutChart",
    "pieChart":     "pieChart",
    "scatterChart": "scatterChart",
    "card":         "card",
    "table":        "tableEx",
    "matrix":       "matrix",
}

PROJ_ROLES = {
    "barChart":     {"category": "Category", "values": "Y"},
    "columnChart":  {"category": "Category", "values": "Y"},
    "lineChart":    {"category": "Category", "values": "Y"},
    "donutChart":   {"category": "Category", "values": "Y"},
    "pieChart":     {"category": "Category", "values": "Y"},
    "scatterChart": {"x": "X", "y": "Y", "details": "Details"},
    "card":         {"values": "Values"},
    "tableEx":      {"category": "Values", "values": "Values"},
    "matrix":       {"category": "Rows",   "values": "Values"},
}

# ─────────────────────────────────────────────
# Role helpers
# ─────────────────────────────────────────────

_DIM_TYPES  = {"text", "string", "boolean", "date", "datetime"}
_MEAS_TYPES = {"number", "integer", "decimal", "float", "currency", "int"}


def _infer_role(col: dict) -> str:
    """Infer role when not explicitly set in catalog."""
    t = col.get("type", "text")
    n = col.get("name", "").lower()
    if t in _DIM_TYPES:
        return "dimension"
    if t in _MEAS_TYPES and any(k in n for k in ("id", "key", "code", "zip", "lat", "lon", "number")):
        return "ignore"
    if t in _MEAS_TYPES:
        return "measure"
    return "dimension"


def get_role(col: dict) -> str:
    """Return the column role, falling back to inference."""
    return col.get("role") or _infer_role(col)


def dimension_cols(catalog: dict) -> list:
    return [c for c in catalog["columns"] if get_role(c) == "dimension"]


def measure_cols(catalog: dict) -> list:
    return [c for c in catalog["columns"] if get_role(c) == "measure"]


def text_dims(catalog: dict) -> list:
    return [c for c in dimension_cols(catalog) if c.get("type") in ("text", "string")]


def date_dims(catalog: dict) -> list:
    return [c for c in dimension_cols(catalog) if c.get("type") in ("date", "datetime")]