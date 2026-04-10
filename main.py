# ─────────────────────────────────────────────
# main.py  —  CLI entry point · project builder
# ─────────────────────────────────────────────
# Usage:
#   python main.py --infer path/to/data.csv
#   python main.py --catalog catalog.json --csv data.csv --output ./out
# ─────────────────────────────────────────────

import os
import json
import shutil
import argparse

from catalog  import (
    validate_catalog, prepare_catalog, build_readme,
    is_intent_catalog, translate_intent_to_catalog
)
from builders import (
    build_pbip, build_platform, build_definition_pbism,
    build_definition_pbir, build_model_bim, build_report_json, build_theme,
)
from inference import infer_catalog_from_csv
from constants import dimension_cols, measure_cols


# ─────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────

def write_json(path: str, data: dict, indent: int = 2) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent)
    print(f"  [created] {os.path.basename(path)}")


def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  [created] {os.path.basename(path)}")


# ─────────────────────────────────────────────
# Project builder
# ─────────────────────────────────────────────

def build_project(catalog: dict, output_dir: str = ".", csv_source_path: str = None) -> None:
    validate_catalog(catalog)

    name         = catalog["project_name"]
    root         = os.path.join(output_dir, name)
    semantic_dir = os.path.join(root, f"{name}.SemanticModel")
    report_dir   = os.path.join(root, f"{name}.Report")
    theme_dir    = os.path.join(report_dir, "StaticResources", "SharedResources", "BaseThemes")

    for d in [root, semantic_dir, report_dir, theme_dir]:
        os.makedirs(d, exist_ok=True)

    print(f"\n[Building project] {name}\n")
    lid_s, lid_r = __import__("uuid").uuid4(), __import__("uuid").uuid4()

    write_json(os.path.join(root, f"{name}.pbip"), build_pbip(name))
    write_text(os.path.join(root, "README.md"),    build_readme(catalog))

    write_json(os.path.join(semantic_dir, ".platform"),        build_platform("SemanticModel", name, str(lid_s)))
    write_json(os.path.join(semantic_dir, "definition.pbism"), build_definition_pbism())
    write_json(os.path.join(semantic_dir, "model.bim"),        build_model_bim(catalog, output_dir))

    if csv_source_path and os.path.exists(csv_source_path):
        dest = os.path.join(semantic_dir, catalog["csv_file"])
        shutil.copy2(csv_source_path, dest)
        print(f"  [copied]  {catalog['csv_file']}  <- {csv_source_path}")
    else:
        print(f"  [missing] CSV not copied — place '{catalog['csv_file']}' in SemanticModel manually")

    write_json(os.path.join(report_dir, ".platform"),       build_platform("Report", name, str(lid_r)))
    write_json(os.path.join(report_dir, "definition.pbir"), build_definition_pbir(name))
    write_json(os.path.join(report_dir, "report.json"),     build_report_json(catalog))

    write_json(os.path.join(theme_dir, "CY24SU10.json"), build_theme())

    abs_csv = os.path.abspath(
        os.path.join(output_dir, name, f"{name}.SemanticModel", catalog["csv_file"])
    )
    print(f"\nDone!  ->  {os.path.abspath(root)}")
    print(f"   CSV path in M query : {abs_csv}")
    print(f"   Dimensions: {len(dimension_cols(catalog))}  |  "
          f"Measures: {len(measure_cols(catalog))}  |  "
          f"Charts: {len(catalog.get('charts', []))}\n")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Power BI .pbip project from catalog.json or a CSV"
    )
    parser.add_argument("--catalog",      "-c", default="catalog.json",
                        help="Path to catalog.json")
    parser.add_argument("--csv",                default=None,
                        help="Path to CSV — auto-copied into SemanticModel")
    parser.add_argument("--infer",              default=None, metavar="CSV_PATH",
                        help="Auto-generate catalog from a CSV (skips --catalog)")
    parser.add_argument("--save-catalog",       action="store_true",
                        help="Save inferred catalog.json to disk")
    parser.add_argument("--output",       "-o", default=".",
                        help="Output directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # ──────────────────────────────────────────────────────────────────────

    catalog, csv_to_copy = None, args.csv

    if args.infer:
        if not os.path.exists(args.infer):
            print(f"ERROR: CSV not found: {args.infer}")
            raise SystemExit(1)
        catalog     = infer_catalog_from_csv(args.infer)
        csv_to_copy = args.infer
        if args.save_catalog:
            with open("catalog.json", "w", encoding="utf-8") as f:
                json.dump({k: v for k, v in catalog.items() if not k.startswith("_")}, f, indent=2)
            print("  Inferred catalog saved -> catalog.json")
    else:
        if not os.path.exists(args.catalog):
            print(f"ERROR: catalog.json not found: {args.catalog}")
            raise SystemExit(1)
        print(f"\n[Reading catalog] {args.catalog}")
        with open(args.catalog, "r", encoding="utf-8") as f:
            catalog = json.load(f)
        
        # ── Intent-based logic ──────────────────────────────────────────
        if is_intent_catalog(catalog):
            print(f"  [Detected] Intent-based catalog format")
            
            # We NEED a CSV for intent interpretation
            csv_for_intent = args.csv or None
            
            # Try to find a CSV in the current directory if not specified
            if not csv_for_intent:
                csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
                if len(csv_files) == 1:
                    csv_for_intent = csv_files[0]
                    print(f"  [Auto-picked] CSV for inference: {csv_for_intent}")
                elif len(csv_files) > 1:
                    print(f"  ⚠ Warning: Multiple CSVs found, please specify one via --csv")
            
            if not csv_for_intent:
                print(f"ERROR: Intent-based catalog requires a CSV for schema inference.")
                print(f"       Please provide --csv path/to/data.csv")
                raise SystemExit(1)
            
            # 1. Infer standard catalog from CSV
            base_catalog = infer_catalog_from_csv(csv_for_intent)
            
            # 2. Translate intent over the inferred catalog
            catalog = translate_intent_to_catalog(catalog, base_catalog)
            csv_to_copy = csv_for_intent
        else:
            csv_to_copy = args.csv or catalog.get("csv_file")

    # Sync csv_file name if the actual file differs
    actual_filename = os.path.basename(csv_to_copy) if csv_to_copy else ""
    if catalog.get("csv_file") and actual_filename and catalog["csv_file"] != actual_filename:
        print(f"  WARNING: catalog csv_file='{catalog['csv_file']}' -> overriding with '{actual_filename}'")
        catalog["csv_file"] = actual_filename

    print(f"\n[Selection phase] picking roles and charts from catalog columns...")
    catalog, was_modified = prepare_catalog(catalog, csv_source_path=csv_to_copy)

    if was_modified and not args.infer:
        with open(args.catalog, "w", encoding="utf-8") as f:
            json.dump({k: v for k, v in catalog.items() if not k.startswith("_")}, f, indent=2)
        print(f"  Selections saved back to: {args.catalog}")

    build_project(catalog, output_dir="C:\\Users\\khair\\OneDrive\\Desktop\\PowerBI\\outputdivise", csv_source_path=csv_to_copy)


if __name__ == "__main__":
    main()