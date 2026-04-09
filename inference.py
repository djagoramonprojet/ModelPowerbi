# ─────────────────────────────────────────────
# inference.py  —  CSV → catalog schema inference
# ─────────────────────────────────────────────

import re
import csv as csvlib

from constants import TYPE_MAP, _infer_role


def infer_type(values: list) -> str:
    clean = [v.strip() for v in values if v.strip() != ""]
    if not clean:
        return "text"

    bool_vals = {"true", "false", "yes", "no", "1", "0"}
    if all(v.lower() in bool_vals for v in clean):
        return "boolean"

    def is_int(v):
        try:
            int(v.replace(",", "").replace(" ", ""))
            return True
        except ValueError:
            return False

    def is_float(v):
        try:
            float(v.replace(",", "").replace(" ", "")
                   .replace("$", "").replace("€", "").replace("%", ""))
            return True
        except ValueError:
            return False

    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{2}/\d{2}/\d{4}$",
        r"^\d{2}-\d{2}-\d{4}$",
        r"^\d{1,2}/\d{1,2}/\d{2,4}$",
    ]
    datetime_patterns = [
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",
        r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}",
    ]

    if all(is_int(v) for v in clean):
        return "integer"
    if all(is_float(v) for v in clean):
        return "number"
    if all(any(re.match(p, v) for p in datetime_patterns) for v in clean):
        return "datetime"
    if all(any(re.match(p, v) for p in date_patterns) for v in clean):
        return "date"
    return "text"


def infer_summarize(col_type: str) -> str:
    return "none" if col_type in ("text", "string", "boolean", "date", "datetime") else "sum"


def infer_format(col_type: str, col_name: str) -> str | None:
    name_lower = col_name.lower()
    if col_type in ("boolean", "date", "datetime", "text", "string"):
        return None
    if "%" in col_name or any(k in name_lower for k in ["margin", "rate", "pct", "percent"]):
        return "0.00%"
    if any(k in name_lower for k in ["price", "revenue", "cost", "profit", "spend",
                                       "salary", "amount", "sales", "income", "budget"]):
        return "$#,##0.00"
    if col_type in ("number", "decimal", "float", "currency"):
        return "#,##0.00"
    if col_type in ("integer", "int"):
        return "#,##0"
    return None


def read_csv_header(csv_path: str) -> list:
    """Read only the header row from a CSV."""
    try:
        with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csvlib.reader(f)
            return next(reader, [])
    except Exception:
        return []


def infer_catalog_from_csv(csv_path: str, sample_rows: int = 100) -> dict:
    print(f"\n[Inferring catalog] {csv_path}")
    import os
    filename     = os.path.basename(csv_path)
    project_name = os.path.splitext(filename)[0].replace(" ", "_").replace("-", "_")

    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        reader  = csvlib.DictReader(f)
        headers = reader.fieldnames or []
        rows    = [row for i, row in enumerate(reader) if i < sample_rows]

    if not headers:
        raise ValueError(f"CSV file has no headers: {csv_path}")

    print(f"  Found {len(headers)} columns, {len(rows)} sample rows")

    columns = []
    for header in headers:
        values   = [row.get(header, "") for row in rows]
        col_type = infer_type(values)
        col_fmt  = infer_format(col_type, header)
        col = {"name": header, "type": col_type, "summarize": infer_summarize(col_type)}
        if col_fmt:
            col["format"] = col_fmt
        col["role"] = _infer_role(col)
        columns.append(col)
        print(f"    {header:<35} → {col_type:<12} role={col['role']}")

    catalog = {
        "project_name": project_name,
        "csv_file":     filename,
        "table_name":   project_name,
        "page_name":    f"{project_name} Dashboard",
        "columns":      columns,
    }
    return catalog