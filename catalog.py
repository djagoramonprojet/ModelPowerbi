# ─────────────────────────────────────────────
# catalog.py  —  validate · enrich · prepare
# ─────────────────────────────────────────────

from constants import (
    TYPE_MAP, get_role,
    dimension_cols, measure_cols, text_dims, date_dims,
)


# ─────────────────────────────────────────────
# Intent-based (Analysis Result) support
# ─────────────────────────────────────────────

def is_intent_catalog(catalog: dict) -> bool:
    """Detect if the catalog is the new 'Intent/Analysis' format."""
    return "data_intent" in catalog or "visualization" in catalog


def translate_intent_to_catalog(intent: dict, inferred_catalog: dict) -> dict:
    """
    Map high-level intent onto a full inferred catalog.
    
    1. Extracts measure, aggregation, and dimensions from data_intent.
    2. Overrides charts with the recommended visualization.
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
    
    # We need to find the column in the inferred catalog that matches the intent measure
    col_names = {c["name"].lower(): c["name"] for c in cat["columns"]}
    target_col = col_names.get(intent_meas)
    
    if not target_col:
        # Fuzzy match
        for cn in col_names:
            if intent_meas in cn:
                target_col = col_names[cn]
                break
    
    if not target_col:
        # Alias match
        possible_aliases = aliases.get(intent_meas, [])
        for alias in possible_aliases:
            if alias in col_names:
                target_col = col_names[alias]
                break
    
    if not target_col and not cat.get("measures"):
        # Fallback to any numeric column
        for c in cat["columns"]:
            if c.get("type") in ("number", "integer", "double", "int64"):
                target_col = c["name"]
                break

    # 2. Update/Create Measure
    primary_meas_name = None
    if target_col:
        primary_meas_name = f"Total {target_col}"
        new_measure = {
            "name":       primary_meas_name,
            "expression": f"{intent_agg}('{cat['table_name']}'[{target_col}])",
            "format":     "$#,##0.00" if any(x in target_col.lower() for x in ("sales", "profit", "revenue", "spend", "cost")) else "#,##0.00"
        }
        # Prepend to measures (create list if missing)
        cat.setdefault("measures", [])
        # Avoid duplicates
        if not any(m["name"] == primary_meas_name for m in cat["measures"]):
            cat["measures"].insert(0, new_measure)
    
    # If we still have no measure name but have measures, pick the first one
    if not primary_meas_name and cat.get("measures"):
        primary_meas_name = cat["measures"][0]["name"]

    # 3. Update Charts
    rec_chart = vis.get("recommended_chart", "").lower()
    
    # Map high-level names to internal chart types
    # "line" -> "lineChart", "bar" -> "barChart", etc.
    type_map = {
        "line": "lineChart",
        "bar": "barChart",
        "column": "columnChart",
        "pie": "pieChart",
        "donut": "donutChart",
        "card": "card",
        "table": "table",
    }
    target_type = type_map.get(rec_chart, "barChart")
    
    # Build the primary chart
    # We need a dimension for breakdown. If intent has dimensions, use them.
    # Otherwise fallback to first text dimension from catalog.
    intent_dims = data_intent.get("dimension", [])
    dim_name = None
    if intent_dims and isinstance(intent_dims, list) and len(intent_dims) > 0:
        dim_name = col_names.get(intent_dims[0].lower())
    
    if not dim_name:
        text_cols = text_dims(cat)
        if text_cols:
            dim_name = text_cols[0]["name"]

    primary_chart = {
        "type": target_type,
        "title": intent.get("explanation", "Data Analysis Result"),
        "x": 20, "y": 20, "width": 800, "height": 450,
        "values": [primary_meas_name] if primary_meas_name else []
    }
    if dim_name and target_type != "card":
        primary_chart["category"] = dim_name

    # Replace auto-generated charts with our specific one + maybe a card
    new_charts = [primary_chart]
    
    # Add a KPI Card if the primary chart isn't a card
    if target_type != "card" and cat.get("measures"):
         new_charts.insert(0, {
            "type": "card", "title": f"Summary of {target_col or intent_meas}",
            "x": 840, "y": 20, "width": 400, "height": 220,
            "values": [cat["measures"][0]["name"]]
        })

    cat["charts"] = new_charts
    cat["page_name"] = "Analysis Result"
    
    return cat


# ─────────────────────────────────────────────
# Auto-generation
# ─────────────────────────────────────────────

def auto_generate_measures(catalog: dict) -> list:
    """
    Generate DAX measures only for columns with role == 'measure'.
    Skips any measure that already exists (case-insensitive).
    """
    table = catalog["table_name"]
    mcols = measure_cols(catalog)
    existing_lowered = {m["name"].lower() for m in catalog.get("measures", [])}
    measures = list(catalog.get("measures", []))

    for col in mcols[:6]:
        auto_name = f"Total {col['name']}"
        if auto_name.lower() not in existing_lowered:
            measures.append({
                "name":       auto_name,
                "expression": f"SUM('{table}'[{col['name']}])",
                "format":     col.get("format", "#,##0.00"),
            })
            existing_lowered.add(auto_name.lower())

    if len(mcols) >= 2:
        n1, n2 = mcols[0]["name"], mcols[1]["name"]
        ratio_name = f"{n1} / {n2} Ratio"
        if ratio_name.lower() not in existing_lowered:
            measures.append({
                "name":       ratio_name,
                "expression": f"DIVIDE(SUM('{table}'[{n1}]), SUM('{table}'[{n2}]), 0)",
                "format":     "0.00",
            })
    return measures


def auto_generate_charts(catalog: dict) -> list:
    """
    Smart rule-based chart generation:
      - KPI Card for the primary measure.
      - Line Chart if a datetime dimension exists.
      - Bar/Pie Charts for text dimensions.
      - Clustered Column if multiple measures exist.
    """
    mcols = measure_cols(catalog)
    dcols = text_dims(catalog)
    tcols = date_dims(catalog)

    if not mcols:
        return []

    dax_names = {
        m["expression"].split("[")[-1].split("]")[0].lower(): m["name"]
        for m in catalog.get("measures", [])
        if "SUM" in m.get("expression", "")
    }

    def get_meas(col_name):
        return dax_names.get(col_name.lower(), col_name)

    charts = []
    positions = [
        (20, 20, 380, 220),
        (420, 20, 780, 220),
        (20, 260, 580, 320),
        (620, 260, 580, 320),
        (20, 600, 580, 320),
        (620, 600, 580, 320),
    ]
    idx = [0]

    def nxt():
        p = positions[idx[0]] if idx[0] < len(positions) else (20, 600 + (idx[0] - 4) * 340, 580, 320)
        idx[0] += 1
        return p

    x, y, w, h = nxt()
    charts.append({
        "type": "card", "title": f"Total {mcols[0]['name']}",
        "x": x, "y": y, "width": w, "height": h,
        "values": [get_meas(mcols[0]["name"])],
    })

    if tcols:
        x, y, w, h = nxt()
        charts.append({
            "type": "lineChart", "title": f"{mcols[0]['name']} Trend",
            "x": x, "y": y, "width": w, "height": h,
            "category": tcols[0]["name"], "values": [get_meas(mcols[0]["name"])],
        })

    for i, dim in enumerate(dcols[:3]):
        m_idx = i % len(mcols)
        m_name = mcols[m_idx]["name"]
        x, y, w, h = nxt()
        ctype = "pieChart" if i == 1 else "barChart"
        charts.append({
            "type": ctype, "title": f"{m_name} by {dim['name']}",
            "x": x, "y": y, "width": w, "height": h,
            "category": dim["name"], "values": [get_meas(m_name)],
        })

    if len(mcols) >= 2 and dcols:
        x, y, w, h = nxt()
        charts.append({
            "type": "columnChart", "title": "Metrics Comparison",
            "x": x, "y": y, "width": w, "height": h,
            "category": dcols[0]["name"],
            "values": [get_meas(m["name"]) for m in mcols[:3]],
        })

    return charts


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────

def validate_catalog(catalog: dict) -> None:
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

    dims = [c["name"] for c in dimension_cols(catalog)]
    meas = [c["name"] for c in measure_cols(catalog)]
    igno = [c["name"] for c in catalog["columns"] if get_role(c) == "ignore"]
    print(f"  Dimensions  ({len(dims)}): {', '.join(dims) or '—'}")
    print(f"  Measures    ({len(meas)}): {', '.join(meas) or '—'}")
    if igno:
        print(f"  Ignored     ({len(igno)}): {', '.join(igno)}")

    measure_names = {m["name"] for m in catalog.get("measures", [])}
    for chart in catalog.get("charts", []):
        if "type" not in chart or "title" not in chart:
            raise ValueError("Every chart needs 'type' and 'title'")
        for v in chart.get("values", []):
            if v not in col_names and v not in measure_names:
                print(f"  ⚠ Warning: chart '{chart['title']}' references unknown '{v}'")

    print("  catalog.json validated successfully")


# ─────────────────────────────────────────────
# Prepare (enrich + assign roles)
# ─────────────────────────────────────────────

def prepare_catalog(catalog: dict, csv_source_path: str = None) -> tuple[dict, bool]:
    """
    Enrich catalog before building:
      1. Store CSV source path.
      2. Auto-infer missing roles.
      3. Auto-generate measures.
      4. Auto-generate charts if absent.
    Returns (catalog, was_modified).
    """
    from constants import _infer_role
    changed = False

    if csv_source_path:
        catalog["_csv_source_path"] = csv_source_path

    for col in catalog["columns"]:
        if "role" not in col or not col["role"]:
            col["role"] = _infer_role(col)
            changed = True

    dims = [c["name"] for c in dimension_cols(catalog)]
    meas = [c["name"] for c in measure_cols(catalog)]
    print(f"  Selection result -> Dimensions: {len(dims)} | Measures: {len(meas)}")

    prev_m = len(catalog.get("measures", []))
    catalog["measures"] = auto_generate_measures(catalog)
    if len(catalog["measures"]) != prev_m:
        changed = True

    if not catalog.get("charts"):
        catalog["charts"] = auto_generate_charts(catalog)
        print(f"  Selected {len(catalog['charts'])} chart types automatically")
        changed = True

    return catalog, changed


# ─────────────────────────────────────────────
# README helper
# ─────────────────────────────────────────────

def build_readme(catalog: dict) -> str:
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