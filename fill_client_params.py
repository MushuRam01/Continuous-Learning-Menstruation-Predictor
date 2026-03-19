#!/usr/bin/env python3
"""Fill per-client parameters across all rows.

For each client (identified by a client column, default: ClientID), this script
propagates that client's first-known values for selected columns to subsequent
rows whenever those fields are blank.

Your dataset is sorted by client and cycle number, so the first row for a client
typically contains the parameters; this script extrapolates them to all cycles.

Examples:
  python3 fill_client_params.py --input dataset_mapped.csv --output dataset_mapped_filled.csv
  python3 fill_client_params.py --inplace --input dataset.csv

By default it only fills missing values. Use --overwrite to force all rows for a
client to match the client's first-known values.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_PARAM_COLS: Tuple[str, ...] = (
    "Age",
    "AgeM",
    "Maristatus",
    "MaristatusM",
    "Yearsmarried",
    "Wedding",
    "Ethnicity",
    "Schoolyears",
    "IncomeM",
    "Height",
    "Weight",
    "Livingkids",
    "Miscarriages",
    "Abortions",
    "Medvits",
    "Breastfeeding",
    "BMI",
)


def _normalize_col_name(name: str) -> str:
    # Handles UTF-8 BOM on the first header cell and trims whitespace.
    return name.lstrip("\ufeff").strip()


def _is_blank(value: str) -> bool:
    return value.strip() == ""


def _sniff_dialect(in_f) -> csv.Dialect:
    try:
        sample = in_f.read(8192)
        in_f.seek(0)
        return csv.Sniffer().sniff(sample)
    except csv.Error:
        in_f.seek(0)
        return csv.excel


def _resolve_col_indices(header: Sequence[str], names: Sequence[str]) -> Tuple[List[int], List[str]]:
    indices: List[int] = []
    missing: List[str] = []
    normalized_to_index: Dict[str, int] = {}
    for idx, col in enumerate(header):
        norm = _normalize_col_name(col)
        if norm and norm not in normalized_to_index:
            normalized_to_index[norm] = idx

    header_set = set(normalized_to_index.keys())
    for name in names:
        norm_name = _normalize_col_name(name)
        if norm_name in header_set:
            indices.append(normalized_to_index[norm_name])
        else:
            missing.append(name)
    return indices, missing


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fill per-client parameters by propagating first-known values within each client."
    )
    parser.add_argument("--input", "-i", default="dataset.csv", help="Input CSV path")
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output CSV path (default: <input>_filled.csv)",
    )
    parser.add_argument(
        "--client-col",
        default="ClientID",
        help="Name of the client identifier column (default: ClientID)",
    )
    parser.add_argument(
        "--cols",
        default=",".join(DEFAULT_PARAM_COLS),
        help="Comma-separated list of columns to extrapolate (defaults to the requested set)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing (non-blank) values to match the client's first-known value",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite the input file (writes via a temporary file first)",
    )

    args = parser.parse_args(argv)

    input_path = args.input
    if args.inplace:
        output_path = input_path
    else:
        output_path = args.output
        if output_path is None:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_filled{ext or '.csv'}"

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    requested_cols = [c.strip() for c in str(args.cols).split(",") if c.strip()]

    # client -> col_idx -> first_known_value
    first_known: Dict[str, Dict[int, str]] = {}
    filled_count = 0
    overwritten_count = 0

    with open(input_path, "r", newline="", encoding="utf-8") as in_f:
        dialect = _sniff_dialect(in_f)
        reader = csv.reader(in_f, dialect)

        try:
            header = next(reader)
        except StopIteration:
            print("Input CSV is empty", file=sys.stderr)
            return 2

        normalized_to_index: Dict[str, int] = {}
        for idx, col in enumerate(header):
            norm = _normalize_col_name(col)
            if norm and norm not in normalized_to_index:
                normalized_to_index[norm] = idx

        client_col_norm = _normalize_col_name(args.client_col)
        if client_col_norm not in normalized_to_index:
            print(
                f"Client column '{args.client_col}' not found. Available columns: {', '.join(header)}",
                file=sys.stderr,
            )
            return 2

        client_idx = normalized_to_index[client_col_norm]
        param_indices, missing_cols = _resolve_col_indices(header, requested_cols)

        if not param_indices:
            print("None of the requested columns exist in this CSV; nothing to do.", file=sys.stderr)
            if missing_cols:
                print(f"Missing columns: {', '.join(missing_cols)}", file=sys.stderr)
            return 2

        if missing_cols:
            print(f"Warning: missing columns skipped: {', '.join(missing_cols)}", file=sys.stderr)

        # Prepare output (temp file if in-place)
        if args.inplace:
            out_dir = os.path.dirname(os.path.abspath(input_path)) or "."
            fd, tmp_path = tempfile.mkstemp(prefix=".tmp_client_fill_", suffix=".csv", dir=out_dir)
            os.close(fd)
            final_path = output_path
            out_path = tmp_path
        else:
            out_path = output_path
            final_path = None

        try:
            with open(out_path, "w", newline="", encoding="utf-8") as out_f:
                writer = csv.writer(out_f, dialect)
                writer.writerow([_normalize_col_name(h) for h in header])

                for row in reader:
                    if not row:
                        writer.writerow(row)
                        continue

                    # Ensure row length at least header length (csv rows can be ragged)
                    if len(row) < len(header):
                        row.extend([""] * (len(header) - len(row)))

                    client = row[client_idx]
                    if client not in first_known:
                        first_known[client] = {}

                    known_for_client = first_known[client]

                    # Update first-known values when we see a non-blank cell
                    for col_idx in param_indices:
                        val = row[col_idx]
                        if col_idx not in known_for_client and not _is_blank(val):
                            known_for_client[col_idx] = val

                    # Apply fill/overwrite using first-known values
                    for col_idx in param_indices:
                        if col_idx not in known_for_client:
                            continue
                        if args.overwrite:
                            if row[col_idx] != known_for_client[col_idx]:
                                row[col_idx] = known_for_client[col_idx]
                                overwritten_count += 1
                        else:
                            if _is_blank(row[col_idx]):
                                row[col_idx] = known_for_client[col_idx]
                                filled_count += 1

                    writer.writerow(row)

            if final_path is not None:
                os.replace(out_path, final_path)

        finally:
            if args.inplace and final_path is not None:
                if os.path.exists(out_path) and os.path.abspath(out_path) != os.path.abspath(final_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass

    print(f"Wrote: {output_path}")
    print(f"Clients seen: {len(first_known)}")
    if args.overwrite:
        print(f"Cells overwritten: {overwritten_count}")
    else:
        print(f"Cells filled: {filled_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
