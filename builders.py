# ─────────────────────────────────────────────
# builders.py  —  all PBIP file format builders
# ─────────────────────────────────────────────

import json
import uuid

from constants import (
    TYPE_MAP, PQUERY_TYPE_MAP, VISUAL_TYPE_MAP, PROJ_ROLES,
    get_role, dimension_cols, measure_cols,
)
from inference import read_csv_header


# ─────────────────────────────────────────────
# Top-level structure files
# ─────────────────────────────────────────────

def build_pbip(name: str) -> dict:
    return {
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{name}.Report"}}],
        "settings": {"enableAutoRecovery": True},
    }


def build_platform(artifact_type: str, display_name: str, logical_id: str) -> dict:
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": artifact_type, "displayName": display_name},
        "config":   {"version": "2.0", "logicalId": logical_id},
    }


def build_definition_pbism() -> dict:
    return {"version": "1.0"}


def build_definition_pbir(name: str) -> dict:
    return {
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{name}.SemanticModel"}},
    }


def build_theme() -> dict:
    return {
        "name": "CY24SU10",
        "dataColors": ["#118DFF", "#12239E", "#E66C37", "#6B007B", "#E044A7",
                       "#744EC2", "#D9B300", "#D64550", "#197278", "#1AAB40"],
        "background": "#FFFFFF", "foreground": "#252423", "tableAccent": "#118DFF",
    }


# ─────────────────────────────────────────────
# model.bim builder
# ─────────────────────────────────────────────

def build_model_bim(catalog: dict, output_dir: str) -> dict:
    import os
    table_name   = catalog["table_name"]
    project_name = catalog["project_name"]
    csv_file     = catalog["csv_file"]
    measures     = catalog.get("measures", [])

    active_cols  = [c for c in catalog["columns"] if get_role(c) != "ignore"]
    active_names = [c["name"] for c in active_cols]

    abs_csv_path = os.path.abspath(
        os.path.join(output_dir, project_name, f"{project_name}.SemanticModel", csv_file)
    )
    m_csv_path = abs_csv_path.replace("\\", "\\\\")

    csv_headers = read_csv_header(abs_csv_path)
    if not csv_headers:
        src = catalog.get("_csv_source_path", "")
        if src:
            csv_headers = read_csv_header(src)
    total_csv_columns = len(csv_headers) if csv_headers else len(catalog["columns"])
    print(f"  CSV columns detected: {total_csv_columns}  |  Catalog active columns: {len(active_cols)}")

    bim_columns = []
    for col in active_cols:
        c = {
            "name":         col["name"],
            "dataType":     TYPE_MAP.get(col["type"], "string"),
            "sourceColumn": col["name"],
            "summarizeBy":  col.get("summarize", "none"),
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
    select_list = "{" + ", ".join(f'"{n}"' for n in active_names) + "}"

    m_expression = [
        "let",
        f'    Source = Csv.Document(File.Contents("{m_csv_path}"), [Delimiter=",", Columns={total_csv_columns}, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),',
        "    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),",
        f"    SelectedColumns = Table.SelectColumns(PromotedHeaders, {select_list}),",
        f"    ChangedTypes = Table.TransformColumnTypes(SelectedColumns, {{{type_transforms}}})",
        "in",
        "    ChangedTypes",
    ]

    bim_table = {
        "name":    table_name,
        "columns": bim_columns,
        "partitions": [{
            "name": f"{table_name}-partition",
            "mode": "import",
            "source": {"type": "m", "expression": m_expression},
        }],
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
            "annotations": [{"name": "PBI_QueryOrder", "value": f'["{table_name}"]'}],
        },
    }


# ─────────────────────────────────────────────
# Visual config builder
# ─────────────────────────────────────────────

def _col_sel(entity: str, col_name: str, alias: str = "t") -> dict:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property":   col_name,
        },
        "Name": f"{entity}.{col_name}",
    }


def _meas_sel(entity: str, meas_name: str, alias: str = "t") -> dict:
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Source": alias}},
            "Property":   meas_name,
        },
        "Name": f"{entity}.{meas_name}",
    }


def _agg_sel(entity: str, col_name: str, fn: int = 0, alias: str = "t") -> dict:
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


def _get_best_measure(col_name: str, catalog: dict) -> str | None:
    for m in catalog.get("measures", []):
        expr = m.get("expression", "").upper()
        if "SUM" in expr and f"[{col_name.upper()}]" in expr.replace("'", ""):
            return m["name"]
    return None


def build_visual_config(chart: dict, catalog: dict, z: int) -> dict:
    table    = catalog["table_name"]
    dax_meas = {m["name"] for m in catalog.get("measures", [])}
    ct       = chart["type"]
    pbi_type = VISUAL_TYPE_MAP.get(ct, ct)
    roles    = PROJ_ROLES.get(pbi_type, PROJ_ROLES.get(ct, {"category": "Category", "values": "Y"}))
    title    = chart["title"]
    x, y     = chart.get("x", 20), chart.get("y", 20)
    w, h     = chart.get("width", 580), chart.get("height", 300)
    vname    = f"v{abs(hash(title)) % 10_000_000:07d}"
    sels, proj = [], {}

    def add_dim(col_name, bucket):
        s = _col_sel(table, col_name)
        sels.append(s)
        proj.setdefault(bucket, []).append({"queryRef": s["Name"]})

    def add_val(col_name, bucket):
        best_m = _get_best_measure(col_name, catalog) if col_name not in dax_meas else None
        target = best_m if best_m else col_name
        s = _meas_sel(table, target) if target in dax_meas else _agg_sel(table, target)
        sels.append(s)
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
        "height":  h, "width": w,
        "x": x, "y": y, "z": z,
    }


# ─────────────────────────────────────────────
# report.json builder
# ─────────────────────────────────────────────

def build_report_json(catalog: dict) -> dict:
    charts = catalog.get("charts", [])
    vc  = [build_visual_config(c, catalog, (i + 1) * 1000) for i, c in enumerate(charts)]
    ch  = max((c.get("y", 0) + c.get("height", 300) for c in charts), default=720) + 20
    cw  = max((c.get("x", 0) + c.get("width", 580)  for c in charts), default=1280)
    return {
        "id": str(uuid.uuid4()),
        "layoutOptimization": "None",
        "pods": [],
        "resourcePackages": [{"resourcePackage": {
            "disabled": False,
            "items": [{"path": "BaseThemes/CY24SU10.json", "type": 202}],
            "name": "SharedResources", "type": 2,
        }}],
        "sections": [{
            "displayName": catalog.get("page_name", "Report"),
            "displayOption": 1,
            "filters": "[]",
            "height": ch,
            "name": "ReportSection1",
            "ordinal": 0,
            "visualContainers": vc,
            "width": cw,
        }],
        "theme": {"name": "CY24SU10", "reportVersionNumber": 5, "type": 2},
    }