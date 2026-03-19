#!/usr/bin/env python3
"""Map client identifiers to sequential integers.

This script rewrites a CSV column (default: ClientID) so that the first
distinct client encountered becomes 1, the second becomes 2, etc. The mapping
is based on first appearance in the file (stable and order-preserving).

Example:
  python3 map_client_ids.py --input dataset.csv --output dataset_mapped.csv
  python3 map_client_ids.py --inplace
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from typing import Dict, Iterable, List, Optional


def _open_csv_reader(path: str) -> tuple[csv.Dialect, csv.reader]:
    # newline='' is required for csv module correctness
    f = open(path, "r", newline="", encoding="utf-8")
    try:
        sample = f.read(8192)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        f.seek(0)
        dialect = csv.excel
    return dialect, csv.reader(f, dialect)


def _resolve_client_col(header: List[str], requested: Optional[str]) -> int:
    if requested is not None:
        if requested not in header:
            raise ValueError(
                f"Client column '{requested}' not found. Available columns: {', '.join(header)}"
            )
        return header.index(requested)

    for candidate in ("ClientID", "client", "client_id", "Client", "CLIENT"):
        if candidate in header:
            return header.index(candidate)

    # Fallback: first column
    return 0


def map_clients(
    rows: Iterable[List[str]], client_col_idx: int
) -> tuple[Iterable[List[str]], Dict[str, int]]:
    mapping: Dict[str, int] = {}

    def gen() -> Iterable[List[str]]:
        next_id = 1
        for row in rows:
            if client_col_idx >= len(row):
                yield row
                continue
            raw_client = row[client_col_idx]
            if raw_client not in mapping:
                mapping[raw_client] = next_id
                next_id += 1
            row[client_col_idx] = str(mapping[raw_client])
            yield row

    return gen(), mapping


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replace client IDs with sequential integers in first-seen order."
    )
    parser.add_argument("--input", "-i", default="dataset.csv", help="Input CSV path")
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output CSV path (default: dataset_mapped.csv next to input)",
    )
    parser.add_argument(
        "--client-col",
        default=None,
        help="Name of the client column (default: auto-detect, prefers 'ClientID')",
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
            output_path = f"{base}_mapped{ext or '.csv'}"

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    mapping: Dict[str, int] = {}

    # Read and write in a streaming way (no pandas dependency)
    with open(input_path, "r", newline="", encoding="utf-8") as in_f:
        try:
            sample = in_f.read(8192)
            in_f.seek(0)
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            in_f.seek(0)
            dialect = csv.excel

        reader = csv.reader(in_f, dialect)
        try:
            header = next(reader)
        except StopIteration:
            print("Input CSV is empty", file=sys.stderr)
            return 2

        client_col_idx = _resolve_client_col(header, args.client_col)

        # Prepare output (temp file if in-place)
        if args.inplace:
            out_dir = os.path.dirname(os.path.abspath(input_path)) or "."
            fd, tmp_path = tempfile.mkstemp(prefix=".tmp_client_map_", suffix=".csv", dir=out_dir)
            os.close(fd)
            final_path = output_path
            out_path = tmp_path
        else:
            out_path = output_path
            final_path = None

        try:
            with open(out_path, "w", newline="", encoding="utf-8") as out_f:
                writer = csv.writer(out_f, dialect)
                writer.writerow(header)

                mapped_rows, mapping = map_clients(reader, client_col_idx)
                for row in mapped_rows:
                    writer.writerow(row)

            if final_path is not None:
                os.replace(out_path, final_path)

        finally:
            # If something failed before replace, ensure tmp is cleaned up
            if args.inplace and final_path is not None:
                # If replace succeeded, out_path no longer exists.
                if os.path.exists(out_path) and os.path.abspath(out_path) != os.path.abspath(final_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass

    print(f"Wrote: {output_path}")
    print(f"Unique clients mapped: {len(mapping)}")
    # Print a small preview of the mapping
    preview = list(mapping.items())[:10]
    if preview:
        print("First mappings:")
        for k, v in preview:
            print(f"  {k} -> {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
