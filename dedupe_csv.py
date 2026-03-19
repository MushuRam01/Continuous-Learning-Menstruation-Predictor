#!/usr/bin/env python3
"""Remove duplicate rows from a CSV.

By default, considers the entire row (all columns) when identifying duplicates.
Keeps the first occurrence and preserves original row order.

Examples:
  python3 dedupe_csv.py --input dataset_mapped_filled.csv --output dataset_mapped_filled_deduped.csv
  python3 dedupe_csv.py --input dataset_mapped_filled.csv --inplace

Notes:
- Uses a streaming reader/writer, but keeps a set of seen row hashes in memory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import tempfile
from typing import Iterable, List, Optional, Sequence


def _sniff_dialect(in_f) -> csv.Dialect:
    try:
        sample = in_f.read(8192)
        in_f.seek(0)
        return csv.Sniffer().sniff(sample)
    except csv.Error:
        in_f.seek(0)
        return csv.excel


def _row_key(row: Sequence[str], *, normalize_ws: bool) -> bytes:
    # Use a stable delimiter that cannot appear in CSV fields as a character boundary.
    # (Fields can contain any text, but \x1f is extremely unlikely; plus we hash bytes.)
    if normalize_ws:
        parts = [c.strip() for c in row]
    else:
        parts = list(row)

    joined = "\x1f".join(parts)
    return joined.encode("utf-8", errors="surrogatepass")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Remove duplicate rows from a CSV (keep first occurrence).")
    parser.add_argument("--input", "-i", default="dataset_mapped_filled.csv", help="Input CSV path")
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output CSV path (default: <input>_deduped.csv)",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite the input file (writes via a temporary file first)",
    )
    parser.add_argument(
        "--normalize-ws",
        action="store_true",
        help="Treat rows as equal even if cells differ only by surrounding whitespace",
    )

    args = parser.parse_args(argv)

    input_path = args.input
    if args.inplace:
        output_path = input_path
    else:
        output_path = args.output
        if output_path is None:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_deduped{ext or '.csv'}"

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    seen: set[bytes] = set()
    total = 0
    kept = 0

    with open(input_path, "r", newline="", encoding="utf-8") as in_f:
        dialect = _sniff_dialect(in_f)
        reader = csv.reader(in_f, dialect)

        try:
            header = next(reader)
        except StopIteration:
            print("Input CSV is empty", file=sys.stderr)
            return 2

        # Prepare output (temp file if in-place)
        if args.inplace:
            out_dir = os.path.dirname(os.path.abspath(input_path)) or "."
            fd, tmp_path = tempfile.mkstemp(prefix=".tmp_csv_dedupe_", suffix=".csv", dir=out_dir)
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

                for row in reader:
                    total += 1
                    # Ensure row length at least header length to avoid accidental key differences
                    if len(row) < len(header):
                        row = list(row) + [""] * (len(header) - len(row))

                    key_bytes = _row_key(row, normalize_ws=args.normalize_ws)
                    digest = hashlib.blake2b(key_bytes, digest_size=16).digest()

                    if digest in seen:
                        continue
                    seen.add(digest)
                    writer.writerow(row)
                    kept += 1

            if final_path is not None:
                os.replace(out_path, final_path)

        finally:
            if args.inplace and final_path is not None:
                if os.path.exists(out_path) and os.path.abspath(out_path) != os.path.abspath(final_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass

    dropped = total - kept
    print(f"Wrote: {output_path}")
    print(f"Rows read (excluding header): {total}")
    print(f"Rows kept: {kept}")
    print(f"Duplicates dropped: {dropped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
