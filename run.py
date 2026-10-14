"""Run the whole pipeline and open an HTML report of the results.

    python run.py            # ingest, transform, test, open the report
    python run.py --no-open  # same, without opening a browser
"""

import argparse
import os
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

from report.build_report import build

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
DBT = str(Path(PYTHON).with_name("dbt"))

# name, command, working directory
STEPS = [
    ("SMARD-Marktdaten", [PYTHON, "ingestion/smard.py"], ROOT),
    ("Open-Meteo-Wetterdaten", [PYTHON, "ingestion/open_meteo.py"], ROOT),
    ("dbt build", [DBT, "build"], ROOT / "transform"),
]


def run_step(name, command, cwd):
    print(f"• {name} ...", end=" ", flush=True)
    start = time.monotonic()
    result = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True,
        env={**os.environ, "DBT_USE_COLORS": "false"},
    )
    seconds = round(time.monotonic() - start, 1)
    ok = result.returncode == 0
    print("ok" if ok else "FEHLER")
    output = result.stdout.strip() if ok else (result.stdout + result.stderr).strip()
    return {
        "name": name,
        "status": "ok" if ok else "fail",
        "seconds": seconds,
        # Keep the report short: the last lines tell what happened
        "output": "\n".join(output.splitlines()[-12:]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-open", action="store_true", help="do not open the report in a browser")
    args = parser.parse_args()

    steps = []
    failed = False
    for name, command, cwd in STEPS:
        if failed:
            # A later step would work on incomplete data, so stop here
            steps.append({"name": name, "status": "skip", "seconds": None, "output": ""})
            continue
        step = run_step(name, command, cwd)
        steps.append(step)
        failed = step["status"] == "fail"

    path = build(steps)
    print(f"\nBericht: {path}")
    if not args.no_open:
        webbrowser.open(path.as_uri())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
