#!/usr/bin/env python
"""
1:1 comparison of the streaming output against the reference Feather file.

    python tests/compare_outputs.py
    python tests/compare_outputs.py --max-columns 50        # quick look
    python tests/compare_outputs.py --reference A --candidate B

Compares:
  1. schema  -- column names, order and Arrow types
  2. rows    -- row count and the ID sequence (row order)
  3. values  -- every column, one at a time

Columns are read one at a time from each file, so peak memory is ~2 columns
(about 70 MB at 4.4M rows) rather than the ~13 GB a full decompression of both
files would take. Exit code is 0 when the files match, 1 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from paths import DATA_OUT  # noqa: E402

DEFAULT_REFERENCE = Path(DATA_OUT) / "DHSBirthsGlobal&ClimateShocks_v11_full.feather"
DEFAULT_CANDIDATE = Path(DATA_OUT) / "DHSBirthsGlobal&ClimateShocks_v11_full_dask.feather"

# Documented, intentional differences (see header note 2 of the merge script).
EXPECTED_EXTRA_COLUMNS = {"ID_country", "IDsurvey_country"}


def read_schema(path):
    """Schema only -- reads the IPC footer, not the data."""
    with pa.memory_map(str(path), "rb") as source:
        return pa.ipc.open_file(source).schema


def read_column(path, name):
    return feather.read_table(path, columns=[name]).column(name)


def compare_schemas(ref_schema, cand_schema):
    """Returns (shared_columns, problems)."""
    problems = []
    ref_names, cand_names = ref_schema.names, cand_schema.names

    missing = [c for c in ref_names if c not in set(cand_names)]
    extra = [c for c in cand_names if c not in set(ref_names)]

    if missing:
        problems.append(f"{len(missing)} column(s) missing from candidate: {missing[:10]}")
    if extra:
        unexpected = [c for c in extra if c not in EXPECTED_EXTRA_COLUMNS]
        expected = [c for c in extra if c in EXPECTED_EXTRA_COLUMNS]
        if expected:
            print(f"  note: candidate adds {expected} (--keep-country-ids; documented)")
        if unexpected:
            problems.append(
                f"{len(unexpected)} unexpected extra column(s): {unexpected[:10]}"
            )

    shared = [c for c in ref_names if c in set(cand_names)]
    if [c for c in cand_names if c in set(ref_names)] != shared:
        problems.append("shared columns are in a different order")

    type_mismatches = [
        (c, str(ref_schema.field(c).type), str(cand_schema.field(c).type))
        for c in shared
        if ref_schema.field(c).type != cand_schema.field(c).type
    ]
    if type_mismatches:
        problems.append(f"{len(type_mismatches)} column(s) differ in dtype:")
        for name, a, b in type_mismatches[:10]:
            problems.append(f"    {name}: reference={a}  candidate={b}")

    return shared, problems


def row_permutation(ref_ids, cand_ids):
    """
    None when the row order already matches; otherwise the index array that
    reorders the candidate onto the reference. Raises when the row SETS differ.
    """
    if len(ref_ids) != len(cand_ids):
        raise ValueError(f"row count differs: {len(ref_ids)} vs {len(cand_ids)}")
    if np.array_equal(ref_ids, cand_ids):
        return None
    positions = pd.Index(cand_ids).get_indexer(ref_ids)
    if (positions < 0).any():
        raise ValueError(
            f"{int((positions < 0).sum())} reference ID(s) absent from the candidate"
        )
    return positions


def compare_column(name, ref_path, cand_path, permutation):
    """Returns None when identical, else a one-line description."""
    ref_col = read_column(ref_path, name)
    cand_col = read_column(cand_path, name)
    if permutation is not None:
        cand_col = pc.take(cand_col, pa.array(permutation))
    try:
        pd.testing.assert_series_equal(
            ref_col.to_pandas().reset_index(drop=True),
            cand_col.to_pandas().reset_index(drop=True),
            check_names=False,
        )
    except AssertionError as exc:
        return str(exc).strip().splitlines()[0]
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", default=str(DEFAULT_REFERENCE))
    parser.add_argument("--candidate", default=str(DEFAULT_CANDIDATE))
    parser.add_argument("--max-columns", type=int, default=None,
                        help="only check the first N shared columns")
    parser.add_argument("--stop-after", type=int, default=20,
                        help="stop listing value differences after N columns")
    args = parser.parse_args(argv)

    ref_path, cand_path = Path(args.reference), Path(args.candidate)
    for path in (ref_path, cand_path):
        if not path.exists():
            print(f"FAIL: {path} does not exist")
            return 1

    print(f"reference: {ref_path}")
    print(f"candidate: {cand_path}\n")

    print("1. schema")
    ref_schema, cand_schema = read_schema(ref_path), read_schema(cand_path)
    print(f"  reference {len(ref_schema.names)} columns, candidate {len(cand_schema.names)}")
    shared, problems = compare_schemas(ref_schema, cand_schema)
    for line in problems:
        print(f"  DIFF {line}")
    if not problems:
        print("  OK  identical column names, order and dtypes")

    print("\n2. rows")
    ref_ids = read_column(ref_path, "ID").to_numpy(zero_copy_only=False)
    cand_ids = read_column(cand_path, "ID").to_numpy(zero_copy_only=False)
    print(f"  reference {len(ref_ids):,} rows, candidate {len(cand_ids):,}")
    try:
        permutation = row_permutation(ref_ids, cand_ids)
    except ValueError as exc:
        print(f"  DIFF {exc}")
        print("\nRESULT: files do NOT match")
        return 1
    if permutation is None:
        print("  OK  identical row count and row order")
    else:
        print("  NOTE same rows, different order -- values compared after aligning on ID")

    print("\n3. values")
    columns = shared[: args.max_columns] if args.max_columns else shared
    mismatches = []
    for index, name in enumerate(columns, start=1):
        if index % 100 == 0 or index == len(columns):
            print(f"  ... {index}/{len(columns)} columns", end="\r", flush=True)
        problem = compare_column(name, ref_path, cand_path, permutation)
        if problem is not None:
            mismatches.append((name, problem))
            if len(mismatches) >= args.stop_after:
                print(f"\n  stopping after {args.stop_after} differing columns")
                break
    print(" " * 40, end="\r")

    if mismatches:
        print(f"  DIFF {len(mismatches)} of {len(columns)} columns differ:")
        for name, problem in mismatches:
            print(f"    {name}: {problem}")
    else:
        print(f"  OK  all {len(columns)} columns identical")

    matched = not problems and not mismatches
    print("\nRESULT:", "files match 1:1" if matched else "files do NOT match")
    return 0 if matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
