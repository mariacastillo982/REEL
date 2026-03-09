"""
extract_gold_standard.py - Export gold standard annotations to CSV files.

Produces one CSV per dataset/subset with columns:
    doc_id, entity_text, gold_id

Output files (written to --out_dir, default: gold_standard/):
    gold_standard_craft_chebi.csv
    gold_standard_bc5cdr_chemicals_train.csv
    gold_standard_bc5cdr_chemicals_dev.csv
    gold_standard_bc5cdr_chemicals_test.csv
    gold_standard_bc5cdr_chemicals_all.csv
    gold_standard_bc5cdr_medic_train.csv
    gold_standard_bc5cdr_medic_dev.csv
    gold_standard_bc5cdr_medic_test.csv
    gold_standard_bc5cdr_medic_all.csv

Usage:
    python extract_gold_standard.py
    python extract_gold_standard.py --out_dir my_gold_dir/
    python extract_gold_standard.py --datasets craft_chebi bc5cdr_chemicals bc5cdr_medic
"""

import argparse
import csv
import os
import sys

sys.path.append("./")

from src.annotations import parse_craft_chebi_annotations, parse_cdr_annotations_pubtator

FIELDNAMES = ["doc_id", "entity_text", "gold_id"]


def normalise_chebi_id(chebi_id):
    """Convert 'ChEBI_XXXXX' → 'CHEBI:XXXXX' for consistency."""
    return chebi_id.replace("ChEBI_", "CHEBI:").replace("CHEBI_", "CHEBI:")


def write_csv(filepath, annotations, id_transform=None):
    """Write {doc_id: [(gold_id, text), ...]} to a CSV file.

    Args:
        filepath: destination CSV path
        annotations: dict returned by any parse_* function
        id_transform: optional callable applied to each gold_id before writing
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    row_count = 0

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for doc_id in sorted(annotations.keys()):
            for gold_id, entity_text in annotations[doc_id]:
                normalised_id = id_transform(gold_id) if id_transform else gold_id
                writer.writerow({
                    "doc_id": doc_id,
                    "entity_text": entity_text,
                    "gold_id": normalised_id,
                })
                row_count += 1

    return row_count


def extract_craft_chebi(out_dir):
    print("Extracting CRAFT-ChEBI ...", end=" ", flush=True)
    annotations = parse_craft_chebi_annotations()
    filepath = os.path.join(out_dir, "gold_standard_craft_chebi.csv")
    count = write_csv(filepath, annotations, id_transform=normalise_chebi_id)
    print(f"{count} annotations → {filepath}")


def extract_bc5cdr(entity_type, label, out_dir):
    subsets = ["train", "dev", "test", "all"]
    for subset in subsets:
        print(f"Extracting BC5CDR-{label} ({subset}) ...", end=" ", flush=True)
        annotations = parse_cdr_annotations_pubtator(entity_type, subset)
        filename = f"gold_standard_bc5cdr_{label.lower()}_{subset}.csv"
        filepath = os.path.join(out_dir, filename)
        count = write_csv(filepath, annotations)
        print(f"{count} annotations → {filepath}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export REEL gold standard datasets to CSV files."
    )
    parser.add_argument(
        "--out_dir",
        default="gold_standard",
        help="Directory to write CSV files (default: gold_standard/)",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=["craft_chebi", "bc5cdr_chemicals", "bc5cdr_medic"],
        default=["craft_chebi", "bc5cdr_chemicals", "bc5cdr_medic"],
        help="Datasets to extract (default: all three)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = args.out_dir

    print(f"Writing gold standard CSVs to: {out_dir}/\n")

    if "craft_chebi" in args.datasets:
        extract_craft_chebi(out_dir)

    if "bc5cdr_chemicals" in args.datasets:
        extract_bc5cdr("Chemical", "chemicals", out_dir)

    if "bc5cdr_medic" in args.datasets:
        extract_bc5cdr("Disease", "medic", out_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
