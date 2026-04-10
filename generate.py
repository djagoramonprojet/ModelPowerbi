import os
import json
import uuid
import shutil
import argparse
import csv as csvlib


# ─────────────────────────────────────────────
# TYPE MAPPINGS
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

# ─────────────────────────────────────────────
# ROLE HELPERS
# Column "role" field in catalog.json can be:
#   "dimension" — categorical / date / id columns (used as category axes)
#   "measure"   — numeric columns used for aggregation (Y-axis / values)
#   "ignore"    — excluded from all chart / measure generation
# If no role is specified, role is inferred from the column type.
# ─────────────────────────────────────────────

_DIM_TYPES  = {"text", "string", "boolean", "date", "datetime"}
_MEAS_TYPES = {"number", "integer", "decimal", "float", "currency", "int"}


def _infer_role(col):
    """Infer role when not explicitly set in catalog."""
    t = col.get("type", "text")
    n = col.get("name", "").lower()
    if t in _DIM_TYPES:
        return "dimension"
    # numeric columns whose name looks like an id → dimension
    if t in _MEAS_TYPES and any(k in n for k in ("id", "key", "code", "number")):
        return "dimension"
    if t in _MEAS_TYPES:
        return "measure"
    return "dimension"


def get_role(col):
    """Return the column role, falling back to inference."""
    return col.get("role") or _infer_role(col)


# ─────────────────────────────────────────────
# INTENT-BASED (ANALYSIS RESULT) SUPPORT
# ─────────────────────────────────────────────

def is_intent_catalog(catalog):
    """Detect if the catalog is the new 'Intent/Analysis' format."""
    return "data_intent" in catalog or "visualization" in catalog


def translate_intent_to_catalog(intent, inferred_catalog):
    """
    Map high-level intent onto a full inferred catalog.
    """
    cat = dict(inferred_catalog)
    data_intent = intent.get("data_intent", {})
    vis         = intent.get("visualization", {})
    
    # 1. Get primary measure name and aggregation
    intent_meas = data_intent.get("measure", "sales").lower()
    intent_agg  = data_intent.get("aggregation", "sum").upper()
    
    # Common Aliases
    aliases = {
        "sales": ["profit", "revenue", "amount", "total", "spend", "cost"],
        "revenue": ["sales", "profit", "amount"],
        "profit": ["revenue", "sales", "income"]
    }
    
    # Fuzzy match column name
    col_names = {c["name"].lower(): c["name"] for c in cat["columns"]}
    target_col = col_names.get(intent_meas)
    
    if not target_col:
        for cn in col_names:
            if intent_meas in cn:
                target_col = col_names[cn]; break
    
    if not target_col:
        possible_aliases = aliases.get(intent_meas, [])
        for alias in possible_aliases:
            if alias in col_names:
                target_col = col_names[alias]; break

    if not target_col and not cat.get("measures"):
        for c in cat["columns"]:
            if c.get("type") in ("number", "integer"):
                target_col = c["name"]; break

    # 2. Update/Create Measures
    primary_meas_name = None
    if target_col:
        primary_meas_name = f"Total {target_col}"
        new_measure = {
            "name":       primary_meas_name,
            "expression": f"{intent_agg}('{cat['table_name']}'[{target_col}])",
            "format":     "$#,##0.00" if any(x in target_col.lower() for x in ("sales", "profit", "revenue", "spend", "cost")) else "#,##0.00"
        }
        cat.setdefault("measures", [])
        if not any(m["name"] == primary_meas_name for m in cat["measures"]):
            cat["measures"].insert(0, new_measure)

    if not primary_meas_name and cat.get("measures"):
        primary_meas_name = cat["measures"][0]["name"]

    # 3. Update Charts
    rec_chart = vis.get("recommended_chart", "").lower()
    type_map = {
        "line": "lineChart", "bar": "barChart", "column": "columnChart",
        "pie": "pieChart", "donut": "donutChart", "card": "card"
    }
    target_type = type_map.get(rec_chart, "barChart")
    
    intent_dims = data_intent.get("dimension", [])
    dim_name = None
    if intent_dims and isinstance(intent_dims, list) and len(intent_dims) > 0:
        dim_name = col_names.get(intent_dims[0].lower())
    
    if not dim_name:
        if target_type == "lineChart":
            d_dims = date_dims(cat)
            if d_dims: dim_name = d_dims[0]["name"]
        
        if not dim_name:
            t_dims = text_dims(cat)
            if t_dims: dim_name = t_dims[0]["name"]

    primary_chart = {
        "type": target_type,
        "title": intent.get("explanation", "Data Analysis Result"),
        "x": 20, "y": 20, "width": 800, "height": 450,
        "values": [primary_meas_name] if primary_meas_name else []
    }
    if dim_name and target_type != "card":
        primary_chart["category"] = dim_name

    new_charts = [primary_chart]
    if target_type != "card" and cat.get("measures"):
         new_charts.insert(0, {
            "type": "card", "title": f"Summary of {target_col or intent_meas}",
            "x": 840, "y": 20, "width": 400, "height": 220,
            "values": [cat["measures"][0]["name"]]
        })

    cat["charts"] = new_charts
    cat["page_name"] = "Analysis Result"
    return cat


def dimension_cols(catalog):
    """Return columns whose role is 'dimension'."""
    return [c for c in catalog["columns"] if get_role(c) == "dimension"]


def measure_cols(catalog):
    """Return columns whose role is 'measure'."""
    return [c for c in catalog["columns"] if get_role(c) == "measure"]


def text_dims(catalog):
    """Dimension columns that are text/string (category axes)."""
    return [c for c in dimension_cols(catalog)
            if c.get("type") in ("text", "string")]


def date_dims(catalog):
    """Dimension columns that are date/datetime (time axes)."""
    return [c for c in dimension_cols(catalog)
            if c.get("type") in ("date", "datetime")]


# ─────────────────────────────────────────────
# CSV INFERENCE ENGINE
# ─────────────────────────────────────────────

def infer_type(values):
    import re
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

    def is_date(v):
        return any(re.match(p, v) for p in date_patterns)

    def is_datetime(v):
        return any(re.match(p, v) for p in datetime_patterns)

    if all(is_int(v) for v in clean):
        return "integer"
    if all(is_float(v) for v in clean):
        return "number"
    if all(is_datetime(v) for v in clean):
        return "datetime"
    if all(is_date(v) for v in clean):
        return "date"
    return "text"


def infer_summarize(col_type):
    return "none" if col_type in ("text", "string", "boolean", "date", "datetime") else "sum"


def infer_format(col_type, col_name):
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


def _infer_role(col):
    """
    Assign a role based on type and header name.
    """
    name = col["name"].lower()
    if col["type"] == "datetime" or any(x in name for x in ("date", "time", "year", "month")):
         return "dimension"
    if col["type"] in ("number", "integer"):
        if any(x in name for x in ("id", "key", "code", "zip", "lat", "lon")):
            return "ignore"
        return "measure"
    return "dimension"


# ─────────────────────────────────────────────
# AUTO CHART / MEASURE GENERATION  - uses roles
# ─────────────────────────────────────────────

def auto_generate_charts(catalog):
    """
    Smart rule-based chart generation:
      - KPI Card: for the primary measure.
      - Line Chart: if a datetime dimension exists.
      - Bar/Pie Chart: for text dimensions.
    """
    mcols = measure_cols(catalog)
    dcols = text_dims(catalog)
    tcols = date_dims(catalog)

    if not mcols:
        return []

    # Map column names to DAX measure names if they exist, otherwise use column
    dax_names = {m["expression"].split("[")[-1].split("]")[0].lower(): m["name"] 
                 for m in catalog.get("measures", []) if "SUM" in m.get("expression", "")}
    
    def get_meas(col_name):
        return dax_names.get(col_name.lower(), col_name)

    charts = []
    positions = [
        (20, 20, 380, 220),   (420, 20, 780, 220),
        (20, 260, 580, 320),  (620, 260, 580, 320),
        (20, 600, 580, 320),  (620, 600, 580, 320),
    ]
    idx = [0]
    def nxt():
        p = positions[idx[0]] if idx[0] < len(positions) else (20, 600 + (idx[0]-4)*340, 580, 320)
        idx[0] += 1
        return p

    # 1. Main KPI Card
    x,y,w,h = nxt()
    charts.append({
        "type": "card", "title": f"Total {mcols[0]['name']}",
        "x": x, "y": y, "width": w, "height": h,
        "values": [get_meas(mcols[0]["name"])]
    })

    # 2. Line Chart (Historical Trend)
    if tcols:
        x,y,w,h = nxt()
        charts.append({
            "type": "lineChart", "title": f"{mcols[0]['name']} Trend",
            "x": x, "y": y, "width": w, "height": h,
            "category": tcols[0]["name"], "values": [get_meas(mcols[0]["name"])]
        })

    # 3. Bar/Pie Charts for text dimensions
    for i, dim in enumerate(dcols[:3]):
        m_idx = i % len(mcols)
        m_name = mcols[m_idx]["name"]
        x,y,w,h = nxt()
        ctype = "pieChart" if i == 1 else "barChart"
        charts.append({
            "type": ctype, "title": f"{m_name} by {dim['name']}",
            "x": x, "y": y, "width": w, "height": h,
            "category": dim["name"], "values": [get_meas(m_name)]
        })
    
    # 4. Clustered Column (Multi-measure)
    if len(mcols) >= 2 and dcols:
        x,y,w,h = nxt()
        charts.append({
            "type": "columnChart", "title": "Metrics Comparison",
            "x": x, "y": y, "width": w, "height": h,
            "category": dcols[0]["name"], "values": [get_meas(m["name"]) for m in mcols[:3]]
        })

    return charts

def auto_generate_measures(catalog):
    """
    Generate DAX measures only for columns with role == 'measure'.
    Skips any measure that already exists in catalog['measures'] (case-insensitive).
    """
    table = catalog["table_name"]
    mcols = measure_cols(catalog)
    # Power BI measure names are case-insensitive for uniqueness
    existing_lowered = {m["name"].lower() for m in catalog.get("measures", [])}
    measures = list(catalog.get("measures", []))

    for col in mcols[:6]:
        auto_name = f"Total {col['name']}"
        if auto_name.lower() not in existing_lowered:
            measures.append({
                "name":       auto_name,
                "expression": f"SUM('{table}'[{col['name']}])",
                "format":     col.get("format", "#,##0.00")
            })
            existing_lowered.add(auto_name.lower())

    if len(mcols) >= 2:
        n1, n2 = mcols[0]["name"], mcols[1]["name"]
        ratio_name = f"{n1} / {n2} Ratio"
        if ratio_name.lower() not in existing_lowered:
            measures.append({
                "name":       ratio_name,
                "expression": f"DIVIDE(SUM('{table}'[{n1}]), SUM('{table}'[{n2}]), 0)",
                "format":     "0.00"
            })
    return measures


def infer_catalog_from_csv(csv_path, sample_rows=100):
    print(f"\n[Inferring catalog] {csv_path}")

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
        # Assign inferred role so it is visible in the saved catalog
        col["role"] = _infer_role(col)
        columns.append(col)
        print(f"    {header:<35} → {col_type:<12} role={col['role']}")

    catalog = {
        "project_name": project_name,
        "csv_file":     filename,
        "table_name":   project_name,
        "page_name":    f"{project_name} Dashboard",
        "columns":      columns
    }
    catalog["measures"] = auto_generate_measures(catalog)
    catalog["charts"]   = auto_generate_charts(catalog)

    print(f"  Auto-generated {len(catalog['measures'])} measures, {len(catalog['charts'])} charts")
    print(f"  Catalog inferred successfully")
    return catalog


# ─────────────────────────────────────────────
# VALIDATORS
# ─────────────────────────────────────────────

def validate_catalog(catalog):
    for key in ["project_name", "csv_file", "table_name", "columns"]:
        if key not in catalog:
            raise ValueError(f"catalog.json missing required key: '{key}'")
    if not catalog["columns"]:
        raise ValueError("'columns' list cannot be empty")

    col_names = set()
    for col in catalog["columns"]:
        if "name" not in col:
            raise ValueError("Every column needs a 'name'")
        if "type" not in col:
            raise ValueError(f"Column '{col['name']}' missing 'type'")
        if col["type"] not in TYPE_MAP:
            raise ValueError(f"Column '{col['name']}' unsupported type '{col['type']}'")
        role = get_role(col)
        if role not in ("dimension", "measure", "ignore"):
            raise ValueError(
                f"Column '{col['name']}' has invalid role '{role}'. "
                "Must be 'dimension', 'measure', or 'ignore'."
            )
        col_names.add(col["name"])

    # Print role summary
    dims  = [c["name"] for c in dimension_cols(catalog)]
    meas  = [c["name"] for c in measure_cols(catalog)]
    igno  = [c["name"] for c in catalog["columns"] if get_role(c) == "ignore"]
    print(f"  Dimensions  ({len(dims)}): {', '.join(dims) or '—'}")
    print(f"  Measures    ({len(meas)}): {', '.join(meas) or '—'}")
    if igno:
        print(f"  Ignored     ({len(igno)}): {', '.join(igno)}")

    measure_names = {m["name"] for m in catalog.get("measures", [])}

    for chart in catalog.get("charts", []):
        if "type" not in chart or "title" not in chart:
            raise ValueError("Every chart needs 'type' and 'title'")
        if chart["type"] == "scatterChart":
            for f in ["axis_x", "axis_y"]:
                if f in chart and chart[f] not in col_names and chart[f] not in measure_names:
                    print(f"  ⚠ Warning: chart '{chart['title']}' references unknown column/measure '{chart[f]}'")
        else:
            for v in chart.get("values", []):
                if v not in col_names and v not in measure_names:
                    print(f"  ⚠ Warning: chart '{chart['title']}' references unknown column/measure '{v}'")

    print("  catalog.json validated successfully")


# ─────────────────────────────────────────────
# PBIP FILE BUILDERS
# ─────────────────────────────────────────────

def build_pbip(n):
    return {
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{n}.Report"}}],
        "settings": {"enableAutoRecovery": True}
    }


def build_platform(artifact_type, display_name, logical_id):
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": artifact_type, "displayName": display_name},
        "config":   {"version": "2.0", "logicalId": logical_id}
    }


def build_definition_pbism():
    return {"version": "1.0"}


def build_definition_pbir(n):
    return {
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{n}.SemanticModel"}}
    }


def _read_csv_header(csv_path):
    """Read only the header row from a CSV to get the true column count."""
    try:
        with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csvlib.reader(f)
            headers = next(reader, [])
        return headers
    except Exception:
        return []


def build_model_bim(catalog, output_dir):
    """
    Build model.bim.

    Strategy:
    - Read the ACTUAL CSV header to get the true column count (so Power Query
      never truncates the parse regardless of how many columns are in catalog).
    - PromoteHeaders → SelectColumns (keep only catalog columns) → TransformTypes.
    - Columns with role='ignore' are excluded from the final model.
    """
    table_name   = catalog["table_name"]
    project_name = catalog["project_name"]
    csv_file     = catalog["csv_file"]
    measures     = catalog.get("measures", [])

    # Only non-ignored columns land in the model
    active_cols = [c for c in catalog["columns"] if get_role(c) != "ignore"]
    active_names = [c["name"] for c in active_cols]

    abs_csv_path = os.path.abspath(
        os.path.join(output_dir, project_name, f"{project_name}.SemanticModel", csv_file)
    )
    m_csv_path = abs_csv_path.replace("\\", "\\\\")

    # --- Try to read the actual CSV header for the true column count ----------
    csv_headers = _read_csv_header(abs_csv_path)
    if not csv_headers:
        # Pass through the source path if we have it
        src = catalog.get("_csv_source_path", "")
        if src:
            csv_headers = _read_csv_header(src)
    total_csv_columns = len(csv_headers) if csv_headers else len(catalog["columns"])
    print(f"  CSV columns detected: {total_csv_columns}  |  Catalog active columns: {len(active_cols)}")

    bim_columns = []
    for col in active_cols:
        c = {
            "name":         col["name"],
            "dataType":     TYPE_MAP.get(col["type"], "string"),
            "sourceColumn": col["name"],
            "summarizeBy":  col.get("summarize", "none")
        }
        if "format" in col:
            c["formatString"] = col["format"]
        bim_columns.append(c)

    bim_measures = []
    for m in measures:
        bm = {"name": m["name"], "expression": m["expression"]}
        if "format" in m:
            bm["formatString"] = m["format"]
        bim_measures.append(bm)

    type_transforms = ", ".join(
        f'{{\"{c["name"]}\", {PQUERY_TYPE_MAP.get(c["type"], "type text")}}}'
        for c in active_cols
    )

    # Select only the columns defined in the catalog
    select_list = "{" + ", ".join(f'"{n}"' for n in active_names) + "}"

    steps = [
        "let",
        f'    Source = Csv.Document(File.Contents("{m_csv_path}"), [Delimiter=",", Columns={total_csv_columns}, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),',
        "    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),",
        f"    SelectedColumns = Table.SelectColumns(PromotedHeaders, {select_list}),",
        f"    ChangedTypes = Table.TransformColumnTypes(SelectedColumns, {{{type_transforms}}})",
        "in",
        "    ChangedTypes"
    ]

    m_expression = steps

    bim_table = {
        "name":    table_name,
        "columns": bim_columns,
        "partitions": [{
            "name": f"{table_name}-partition",
            "mode": "import",
            "source": {"type": "m", "expression": m_expression}
        }]
    }
    if bim_measures:
        bim_table["measures"] = bim_measures

    return {
        "compatibilityLevel": 1567,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "tables": [bim_table],
            "annotations": [{"name": "PBI_QueryOrder", "value": f'["{table_name}"]'}]
        }
    }


# ─────────────────────────────────────────────
# CHART / REPORT BUILDERS
# ─────────────────────────────────────────────

# Map catalog chart type → Power BI internal visualType name
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

# Projection bucket names used by each Power BI visual type
# Projection bucket names used by each Power BI visual type (Must be EXACT case)
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


def _col_sel(entity, col_name, alias="t"):
    """SELECT entry for a plain dimension column (no aggregation)."""
    return {
        "Column": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property":   col_name,
        },
        "Name": f"{entity}.{col_name}",
    }


def _meas_sel(entity, meas_name, alias="t"):
    """SELECT entry for a DAX measure."""
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property":   meas_name,
        },
        "Name": f"{entity}.{meas_name}",
    }


def _agg_sel(entity, col_name, fn=0, alias="t"):
    """SELECT entry for an aggregated column (Sum by default)."""
    # Note: Power BI Desktop sometimes prefers different capitalization for these internal labels
    # but the 'Name' string is what matters for queryRef.
    lbl = {0: "Sum", 1: "Avg", 2: "Min", 3: "Max", 4: "Count"}.get(fn, "Sum")
    return {
        "Aggregation": {
            "Expression": {
                "Column": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property":   col_name,
                }
            },
            "Function": fn,
        },
        "Name": f"{lbl}({entity}.{col_name})",
    }


def _get_best_measure(col_name, catalog):
    """
    Find an explicit DAX measure that sums this column.
    E.g. for 'sales', find 'Total Sales'.
    """
    for m in catalog.get("measures", []):
        expr = m.get("expression", "").upper()
        # Look for SUM('Table'[Column]) or SUM([Column])
        if "SUM" in expr and f"[{col_name.upper()}]" in expr.replace("'", ""):
            return m["name"]
    return None


def build_visual_config(chart, catalog, z):
    """
    Build a visualContainer dict for report.json.

    Key points vs the old broken version:
      • prototypeQuery now has "Version": 2 (required by Power BI Desktop PBIP parser)
      • projections values are lists of the Select[].Name strings — NOT column names
      • dimension columns use Column select (no aggregation)
      • measure columns / DAX measures use Aggregation / Measure select
      • visualType is mapped through VISUAL_TYPE_MAP to the correct PBI internal name
    """
    table     = catalog["table_name"]
    dax_meas  = {m["name"] for m in catalog.get("measures", [])}
    ct        = chart["type"]
    pbi_type  = VISUAL_TYPE_MAP.get(ct, ct)
    roles     = PROJ_ROLES.get(pbi_type, PROJ_ROLES.get(ct, {"category": "Category", "values": "Y"}))
    title     = chart["title"]
    x, y      = chart.get("x", 20), chart.get("y", 20)
    w, h      = chart.get("width", 580), chart.get("height", 300)
    vname     = f"v{abs(hash(title)) % 10_000_000:07d}"
    sels, proj = [], {}

    def add_dim(col_name, bucket):
        s = _col_sel(table, col_name)
        sels.append(s)
        # VERSION 2: Must be an array of {"queryRef": "..."} objects
        proj.setdefault(bucket, []).append({"queryRef": s["Name"]})

    def add_val(col_name, bucket):
        # SMART BINDING: If this is a raw column but a DAX measure exists for it, use the measure!
        best_m = _get_best_measure(col_name, catalog) if col_name not in dax_meas else None
        target = best_m if best_m else col_name

        s = _meas_sel(table, target) if target in dax_meas else _agg_sel(table, target)
        sels.append(s)
        # VERSION 2: Must be an array of {"queryRef": "..."} objects
        proj.setdefault(bucket, []).append({"queryRef": s["Name"]})

    if ct == "scatterChart":
        ax, ay, det = chart.get("axis_x"), chart.get("axis_y"), chart.get("details")
        if ax:  add_val(ax,  roles.get("x",       "X"))
        if ay:  add_val(ay,  roles.get("y",       "Y"))
        if det: add_dim(det, roles.get("details", "Details"))
    elif ct == "card":
        for v in chart.get("values", []):
            add_val(v, roles.get("values", "Values"))
    elif ct in ("table", "tableEx", "matrix"):
        cat = chart.get("category")
        if cat: add_dim(cat, roles.get("category", "Values"))
        for v in chart.get("values", []):
            add_val(v, roles.get("values", "Values"))
    else:
        # barChart / columnChart / lineChart / donutChart / pieChart
        cat = chart.get("category")
        if cat: add_dim(cat, roles.get("category", "Category"))
        for v in chart.get("values", []):
            add_val(v, roles.get("values", "Y"))

    visual_cfg = {
        "name":    vname,
        "layouts": [{"id": 0, "position": {"x": x, "y": y, "width": w, "height": h, "z": z}}],
        "singleVisual": {
            "visualType":  pbi_type,
            "projections": proj,
            "prototypeQuery": {
                "Version": 2,
                "From":    [{"Name": "t", "Entity": table, "Type": 0}],
                "Select":  sels,
            },
            "vcObjects": {
                "title": [{
                    "properties": {
                        "text":    {"expr": {"Literal": {"Value": f"'{title}'"}}},
                        "visible": {"expr": {"Literal": {"Value": "true"}}},
                    }
                }]
            },
        },
    }

    return {
        "config":  json.dumps(visual_cfg, separators=(",", ":")),
        "filters": "[]",
        "height":  h,
        "width":   w,
        "x":       x,
        "y":       y,
        "z":       z,
    }


def build_report_json(catalog):
    charts = catalog.get("charts", [])
    vc     = [build_visual_config(c, catalog, (i + 1) * 1000) for i, c in enumerate(charts)]
    ch     = max((c.get("y", 0) + c.get("height", 300) for c in charts), default=720) + 20
    cw     = max((c.get("x", 0) + c.get("width", 580)  for c in charts), default=1280)
    return {
        "id": str(uuid.uuid4()),
        "layoutOptimization": "None",
        "pods": [],
        "resourcePackages": [{"resourcePackage": {
            "disabled": False,
            "items": [{"path": "BaseThemes/CY24SU10.json", "type": 202}],
            "name": "SharedResources", "type": 2
        }}],
        "sections": [{
            "displayName": catalog.get("page_name", "Report"),
            "displayOption": 1,
            "filters": "[]",
            "height": ch,
            "name": "ReportSection1",
            "ordinal": 0,
            "visualContainers": vc,
            "width": cw
        }],
        "theme": {"name": "CY24SU10", "reportVersionNumber": 5, "type": 2}
    }


def build_theme():
    return {
        "name": "CY24SU10",
        "dataColors": ["#118DFF", "#12239E", "#E66C37", "#6B007B", "#E044A7",
                       "#744EC2", "#D9B300", "#D64550", "#197278", "#1AAB40"],
        "background": "#FFFFFF", "foreground": "#252423", "tableAccent": "#118DFF"
    }


def build_readme(catalog):
    name  = catalog["project_name"]
    dims  = "\n".join(f"  - {c['name']} ({c['type']}) [dimension]" for c in dimension_cols(catalog))
    meas  = "\n".join(f"  - {c['name']} ({c['type']}) [measure]"   for c in measure_cols(catalog))
    chts  = "\n".join(f"  - {c['title']} ({c['type']})" for c in catalog.get("charts", []))
    daxm  = "\n".join(f"  - {m['name']}" for m in catalog.get("measures", [])) or "  (none)"
    igno  = [c["name"] for c in catalog["columns"] if get_role(c) == "ignore"]
    igno_s = ("\n\n## Ignored columns\n" + "\n".join(f"  - {n}" for n in igno)) if igno else ""
    return (
        f"# {name} — Power BI Project\n\n"
        f"## How to Open\n"
        f"1. Open `{name}.pbip` in Power BI Desktop (March 2026+).\n"
        f"2. Click Refresh.\n\n"
        f"## Dimensions\n{dims or '  (none)'}\n\n"
        f"## Measures (columns)\n{meas or '  (none)'}\n\n"
        f"## DAX Measures\n{daxm}\n\n"
        f"## Charts\n{chts}"
        f"{igno_s}\n"
    )


# ─────────────────────────────────────────────
# FILE WRITERS
# ─────────────────────────────────────────────

def write_json(path, data, indent=2):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent)
    print(f"  [created] {os.path.basename(path)}")


def write_text(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  [created] {os.path.basename(path)}")


# ─────────────────────────────────────────────
# MAIN PROJECT BUILDER
# ─────────────────────────────────────────────

def build_project(catalog, output_dir=".", csv_source_path=None):
    validate_catalog(catalog)

    name         = catalog["project_name"]
    root         = os.path.join(output_dir, name)
    semantic_dir = os.path.join(root, f"{name}.SemanticModel")
    report_dir   = os.path.join(root, f"{name}.Report")
    theme_dir    = os.path.join(report_dir, "StaticResources", "SharedResources", "BaseThemes")

    for d in [root, semantic_dir, report_dir, theme_dir]:
        os.makedirs(d, exist_ok=True)

    print(f"\n[Building project] {name}\n")
    lid_s, lid_r = str(uuid.uuid4()), str(uuid.uuid4())

    write_json(os.path.join(root, f"{name}.pbip"), build_pbip(name))
    write_text(os.path.join(root, "README.md"),    build_readme(catalog))

    write_json(os.path.join(semantic_dir, ".platform"),        build_platform("SemanticModel", name, lid_s))
    write_json(os.path.join(semantic_dir, "definition.pbism"), build_definition_pbism())
    write_json(os.path.join(semantic_dir, "model.bim"),        build_model_bim(catalog, output_dir))

    if csv_source_path and os.path.exists(csv_source_path):
        dest = os.path.join(semantic_dir, catalog["csv_file"])
        shutil.copy2(csv_source_path, dest)
        print(f"  [copied]  {catalog['csv_file']}  <- {csv_source_path}")
    else:
        print(f"  [missing] CSV not copied - place '{catalog['csv_file']}' in SemanticModel manually")

    write_json(os.path.join(report_dir, ".platform"),       build_platform("Report", name, lid_r))
    write_json(os.path.join(report_dir, "definition.pbir"), build_definition_pbir(name))
    write_json(os.path.join(report_dir, "report.json"),     build_report_json(catalog))

    write_json(os.path.join(theme_dir, "CY24SU10.json"), build_theme())

    abs_csv = os.path.abspath(
        os.path.join(output_dir, name, f"{name}.SemanticModel", catalog["csv_file"])
    )
    dims_n = len(dimension_cols(catalog))
    meas_n = len(measure_cols(catalog))
    print(f"\nDone!  ->  {os.path.abspath(root)}")
    print(f"   CSV path in M query : {abs_csv}")
    print(f"   Dimensions: {dims_n}  |  Measures: {meas_n}  |  Charts: {len(catalog.get('charts', []))}\n")


# ─────────────────────────────────────────────
# CATALOG PREPARATION
# ─────────────────────────────────────────────

def prepare_catalog(catalog, csv_source_path=None):
    """
    Enrich catalog before building the project:
      1. Store the CSV source path.
      2. Ensure all columns have a role (auto-infer dimension/measure/ignore).
      3. Auto-generate measures for any measure-role columns.
      4. Auto-generate charts if missing.
    """
    changed = False
    if csv_source_path:
        catalog["_csv_source_path"] = csv_source_path

    # Roles and inference
    for col in catalog["columns"]:
        if "role" not in col or not col["role"]:
            col["role"] = _infer_role(col)
            changed = True

    # ─────────────────────────────────────────────────────────
    # AUTOMATIC CHART REPAIR / COMPLETION
    # If a chart is missing values or category, try to fill them!
    # ─────────────────────────────────────────────────────────
    mcols = measure_cols(catalog)
    dax_meas = [m["name"] for m in catalog.get("measures", [])]
    # Fallback to DAX measures if possible, otherwise use numeric columns
    best_val = dax_meas[0] if dax_meas else (mcols[0]["name"] if mcols else None)

    for chart in catalog.get("charts", []):
        ctype = chart.get("type", "barChart")
        
        # 1. Autopick Values if empty
        if not chart.get("values") and best_val:
            chart["values"] = [best_val]
            changed = True
            print(f"  [Auto-chart] Added values '{best_val}' to '{chart.get('title')}'")

        # 2. Autopick Category if empty (except for Card)
        if ctype != "card" and not chart.get("category"):
            if ctype == "lineChart":
                dates = date_dims(catalog)
                if dates:
                    chart["category"] = dates[0]["name"]
                    changed = True
                else:
                    texts = text_dims(catalog)
                    if texts:
                        chart["category"] = texts[0]["name"]
                        changed = True
            else:
                texts = text_dims(catalog)
                if texts:
                    chart["category"] = texts[0]["name"]
                    changed = True
            
            if chart.get("category"):
                print(f"  [Auto-chart] Added category '{chart['category']}' to '{chart.get('title')}'")

    dims = [c["name"] for c in dimension_cols(catalog)]
    meas = [c["name"] for c in measure_cols(catalog)]
    print(f"  Selection result -> Dimensions: {len(dims)} | Measures: {len(meas)}")

    # Auto-fill measures
    prev_m_count = len(catalog.get("measures", []))
    catalog["measures"] = auto_generate_measures(catalog)
    if len(catalog["measures"]) != prev_m_count:
        changed = True
    
    # Auto-fill charts
    if not catalog.get("charts"):
        catalog["charts"] = auto_generate_charts(catalog)
        print(f"  Selected {len(catalog['charts'])} chart types automatically")
        changed = True

    return catalog, changed


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a Power BI .pbip project from catalog.json or directly from a CSV"
    )
    parser.add_argument("--catalog",      "-c", default="catalog.json", help="Path to catalog.json")
    parser.add_argument("--csv",                default=None,           help="Path to CSV - auto-copied into SemanticModel")
    parser.add_argument("--infer",              default=None,           metavar="CSV_PATH",
                        help="Auto-generate catalog from a CSV (skips --catalog)")
    parser.add_argument("--save-catalog",       action="store_true",    help="Save inferred catalog.json to disk")
    parser.add_argument("--output",       "-o", default=".",            help="Output directory")
    args = parser.parse_args()

    catalog, csv_to_copy = None, args.csv

    # Optional overrides for local workspace ease
    CATALOG_PATH = r"C:/Users/khair/OneDrive/Desktop/PowerBI/catalog.json"
    CSV_PATH     = r"C:/Users/khair/OneDrive/Desktop/PowerBI/data/mock_data.csv"
    OUTPUT_DIR   = r"C:/Users/khair/OneDrive/Desktop/PowerBI/output"

    if os.path.exists(CATALOG_PATH):
        args.catalog = CATALOG_PATH
    if os.path.exists(CSV_PATH) and not args.csv:
        args.csv = CSV_PATH
    if not args.output or args.output == ".":
        args.output = OUTPUT_DIR

    if args.infer:
        if not os.path.exists(args.infer):
            print(f"ERROR: CSV not found: {args.infer}")
            exit(1)
        catalog     = infer_catalog_from_csv(args.infer)
        csv_to_copy = args.infer
    else:
        if not os.path.exists(args.catalog):
            print(f"ERROR: catalog.json not found: {args.catalog}")
            exit(1)
        print(f"\n[Reading catalog] {args.catalog}")
        with open(args.catalog, "r", encoding="utf-8") as f:
            catalog = json.load(f)
        
        # --- Handle Intent format ---
        if is_intent_catalog(catalog):
            print(f"  [Detected] Intent-based catalog format")
            csv_for_intent = args.csv or None
            if not csv_for_intent:
                csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
                if len(csv_files) == 1:
                    csv_for_intent = csv_files[0]
                    print(f"  [Auto-picked] CSV for inference: {csv_for_intent}")
            
            if not csv_for_intent:
                print(f"ERROR: Intent-based catalog requires a CSV. Use --csv")
                exit(1)
            
            base_catalog = infer_catalog_from_csv(csv_for_intent)
            catalog = translate_intent_to_catalog(catalog, base_catalog)
            csv_to_copy = csv_for_intent
        else:
            csv_to_copy = args.csv or catalog.get("csv_file")

    actual_csv_filename = os.path.basename(csv_to_copy) if csv_to_copy else ""
    if catalog.get("csv_file") and actual_csv_filename and catalog.get("csv_file") != actual_csv_filename:
        print(f"  WARNING: catalog csv_file='{catalog.get('csv_file')}' -> overriding with actual file: '{actual_csv_filename}'")
        catalog["csv_file"] = actual_csv_filename

    # Final preparation
    print(f"\n[Selection phase] picking roles and charts from catalog columns...")
    catalog, was_modified = prepare_catalog(catalog, csv_source_path=csv_to_copy)

    # Save the selections back to catalog.json if they were inferred
    if was_modified and not args.infer:
        with open(args.catalog, "w", encoding="utf-8") as f:
            # Clean up internal keys before saving
            save_ver = {k: v for k, v in catalog.items() if not k.startswith("_")}
            json.dump(save_ver, f, indent=2)
        print(f"  Selections saved back to: {args.catalog}")

    build_project(catalog, output_dir=args.output, csv_source_path=csv_to_copy)