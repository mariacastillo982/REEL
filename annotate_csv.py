"""
annotate_csv.py - Annotate a CSV file with ChEBI IDs using the REEL pipeline.

Usage:
    python annotate_csv.py --input_csv chemicals.csv --output_csv chemicals_annotated.csv
    python annotate_csv.py --input_csv data.csv --output_csv out.csv --entity_column compound_name
    python annotate_csv.py --input_csv data.csv --output_csv out.csv --model ppr_ic --link_mode kb_link
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile


def parse_args():
    parser = argparse.ArgumentParser(
        description="Annotate a CSV file with ChEBI IDs using REEL."
    )
    parser.add_argument("--input_csv", required=True, help="Path to input CSV file")
    parser.add_argument("--output_csv", required=True, help="Path to write annotated CSV")
    parser.add_argument(
        "--entity_column",
        default=None,
        help="Column name containing entity/chemical names (default: first column)",
    )
    parser.add_argument(
        "--model",
        default="baseline",
        choices=["baseline", "ppr_ic"],
        help="REEL model to use (default: baseline)",
    )
    parser.add_argument(
        "--link_mode",
        default="none",
        choices=["none", "kb_link", "corpus_link", "kb_corpus_link"],
        help="Graph link mode for candidate scoring (default: none)",
    )
    return parser.parse_args()


def read_csv(input_csv):
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)
    return rows, fieldnames


def build_json_input(rows, entity_col):
    """Convert CSV rows to REEL's expected JSON format: {doc_id: [entity]}."""
    return {f"row_{i + 1}": [row[entity_col]] for i, row in enumerate(rows)}


def run_reel(tmp_json_path, run_label, model, link_mode):
    """Invoke run.py as a subprocess with the temp JSON input file."""
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "run.py"),
        "--run_label", run_label,
        "--input_file", tmp_json_path,
        "-target_kb", "chebi",
        "-model", model,
        "--link_mode", link_mode,
        "--out_dir", f"{run_label}_out.json"
    ]
    subprocess.run(cmd, check=True)


def merge_results(rows, entity_col, results):
    """Add chebi_id to each row from REEL results. REEL lowercases entity keys."""
    for i, row in enumerate(rows):
        doc_id = f"row_{i + 1}"
        entity_key = row[entity_col].lower()
        row["chebi_id"] = results.get(doc_id, {}).get(entity_key, "")
    return rows


def write_csv(output_csv, rows, fieldnames):
    new_fieldnames = fieldnames + ["chebi_id"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()

    # 1. Read input CSV
    rows, fieldnames = read_csv(args.input_csv)
    if not rows:
        print("Error: input CSV is empty.", file=sys.stderr)
        sys.exit(1)

    entity_col = args.entity_column or fieldnames[0]
    if entity_col not in fieldnames:
        print(f"Error: column '{entity_col}' not found in CSV. Available columns: {fieldnames}", file=sys.stderr)
        sys.exit(1)

    print(f"Annotating {len(rows)} rows using column '{entity_col}' against ChEBI...")

    # 2. Build JSON input for REEL
    input_data = build_json_input(rows, entity_col)
    
    # 3. Write temp JSON file
    run_label = os.path.splitext(os.path.basename(args.input_csv))[0]

    with open(f"{run_label}_results.json", "w", encoding="utf-8") as f:
        json.dump(input_data, f)
    tmp_json = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8")
    
    try:
        json.dump(input_data, tmp_json)
        tmp_json.close()
        # 4. Run REEL pipeline
        run_reel(f"{run_label}_results.json", run_label, args.model, args.link_mode)
    finally:
        os.unlink(tmp_json.name)

    # 5. Read results JSON produced by run.py
    results_file = f"{run_label}_results.json"
    """if not os.path.exists(results_file):
        print(f"Error: results file '{results_file}' not found after running REEL.", file=sys.stderr)
        sys.exit(1)"""

    with open(results_file, encoding="utf-8") as f:
        results = json.load(f)
    print(results)
    # 6. Merge ChEBI IDs back into CSV rows
    rows = merge_results(rows, entity_col, results)

    # 7. Write annotated output CSV
    write_csv(args.output_csv, rows, fieldnames)

    matched = sum(1 for row in rows if row["chebi_id"])
    print(f"Done! {matched}/{len(rows)} entities matched. Annotated CSV written to: {args.output_csv}")


if __name__ == "__main__":
    main()
