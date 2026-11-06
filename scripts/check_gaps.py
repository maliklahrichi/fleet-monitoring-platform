"""Looks for holes in the event sequence of one simulator run (the number at the end of event_id)."""
import glob
import sys

import pandas as pd

run_id = sys.argv[1]
files = [f for f in glob.glob("data/parquet/events/**/*.parquet", recursive=True)
         if f.endswith(".parquet")]
frames = [pd.read_parquet(f, columns=["event_id"]) for f in files]
ids = pd.concat(frames).event_id if frames else pd.Series([], dtype=str)
seq = sorted({int(i.rsplit("-", 1)[1]) for i in ids if f"-{run_id}-" in i})
if not seq:
    print("no event for this run id")
    sys.exit()
present = set(seq)
missing = [n for n in range(seq[0], seq[-1] + 1) if n not in present]
longest = run = 0
previous = None
for n in missing:
    run = run + 1 if previous is not None and n == previous + 1 else 1
    longest = max(longest, run)
    previous = n
total = seq[-1] - seq[0] + 1
print(f"run {run_id}: sequence {seq[0]}..{seq[-1]}, {len(seq)} in Parquet, "
      f"{len(missing)} missing ({100 * len(missing) / total:.1f} %), longest hole: {longest}")
