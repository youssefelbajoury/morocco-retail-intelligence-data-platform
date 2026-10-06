"""Command line interface: ``python -m apps.data_generator --scale medium --seed 42``."""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

from mri.synthetic.dq_fixtures import SCENARIOS, write_fixture
from mri.synthetic.generator import generate
from mri.synthetic.profiles import PROFILE_NAMES, get_profile
from mri.synthetic.writer import SUPPORTED_FORMATS, write_dataset

_SYNTHETIC_NOTE = (
    "Controlled synthetic data - realistic relationships modelled explicitly, "
    "NOT statistically equivalent to a real enterprise."
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apps.data_generator",
        description="Generate deterministic controlled synthetic Moroccan retail data (PHASE 02).",
    )
    parser.add_argument(
        "--scale", choices=PROFILE_NAMES, default="medium", help="dataset scale profile"
    )
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    parser.add_argument("--output", default="data/generated", help="output root directory")
    parser.add_argument("--format", choices=SUPPORTED_FORMATS, default="csv", dest="fmt")
    parser.add_argument("--quiet", action="store_true", help="suppress the count report")
    parser.add_argument(
        "--dq-fixture",
        choices=SCENARIOS,
        default=None,
        help="write a controlled data-quality fixture instead of the normal dataset",
    )
    parser.add_argument(
        "--dq-output",
        default="data/dq_fixtures",
        help="output root for data-quality fixtures (never data/generated)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    started = time.perf_counter()

    if args.dq_fixture is not None:
        paths = write_fixture(args.dq_fixture, Path(args.dq_output), args.seed)
        if not args.quiet:
            print(f"data-quality fixture scenario : {args.dq_fixture}")
            print(f"seed                          : {args.seed}")
            print(f"files written                 : {len(paths)}")
            print(f"output                        : {Path(args.dq_output) / args.dq_fixture}")
        return 0

    profile = get_profile(args.scale)
    tables = generate(args.seed, profile)
    manifest = write_dataset(tables, Path(args.output), profile, args.seed, args.fmt)
    elapsed = time.perf_counter() - started

    if not args.quiet:
        width = max(len(entity) for entity in tables)
        print("Morocco Retail Intelligence - synthetic data generator (PHASE 02)")
        print(f"  scale    : {profile.name} (target {profile.sales_label} sales rows)")
        print(f"  seed     : {args.seed}")
        print(f"  format   : {args.fmt}")
        print(f"  period   : {manifest['period']['start']} .. {manifest['period']['end']}")
        print(f"  output   : {Path(args.output) / profile.name}")
        print("")
        print(f"  {'entity':<{width}}  {'rows':>10}")
        for entity, frame in tables.items():
            print(f"  {entity:<{width}}  {len(frame):>10,}")
        total = manifest["total_rows"]
        print(f"  {'TOTAL':<{width}}  {total:>10,}")
        print("")
        print(f"  manifest : {(Path(args.output) / profile.name / 'manifest.json').as_posix()}")
        print(f"  elapsed  : {elapsed:.1f} s")
        print(f"  note     : {_SYNTHETIC_NOTE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
