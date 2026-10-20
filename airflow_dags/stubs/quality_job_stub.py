"""Stand-in for the part C quality job: writes the verdict file described in docs/contracts.md.

Create data/quality/force_fail.flag to simulate a failed quality gate (delete it to pass again).
"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    quality_dir = ROOT / "data" / "quality"
    quality_dir.mkdir(parents=True, exist_ok=True)
    failed = (quality_dir / "force_fail.flag").exists()
    verdict = {
        "date": args.date,
        "passed": not failed,
        "reasons": ["simulated failure (force_fail.flag)"] if failed else [],
        "metrics": {"rows": 0, "duplicate_event_ids": 0, "unknown_region_pct": 0.0},
    }
    out = quality_dir / f"verdict_{args.date}.json"
    out.write_text(json.dumps(verdict, indent=2))
    print(f"wrote {out} (passed={verdict['passed']})")


if __name__ == "__main__":
    main()
