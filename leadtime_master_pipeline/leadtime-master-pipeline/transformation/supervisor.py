"""
Supervisor Transformation (Portfolio Version)

Orchestrates transformation modules in deterministic order.
Runs each module in a separate process and stops on first failure.
"""

from __future__ import annotations
import subprocess
import sys
from pathlib import Path

MODULE_DIR = Path(__file__).parent / "modules"

TASKS = [
    "manageliststyleoforder.py",
    "managepurchaseorder.py",
    "styleproductofplanning.py",
    "mastergroupfabricpotabsofplanning_mastergrouppoitems.py",
    "fabric_trim.py",
    "treatment.py",
    "technical.py",
    "costing.py",
    "cuttingdocket.py",
    "managecostingsheetclient.py",
]

def run_task(name: str) -> int:
    path = MODULE_DIR / name
    print(f"\n============================")
    print(f"[RUN] {name}")
    print(f"============================")
    return subprocess.run([sys.executable, str(path)]).returncode

def main() -> None:
    for t in TASKS:
        rc = run_task(t)
        if rc != 0:
            print("\n[STOP] Supervisor halted due to failure:", t)
            raise SystemExit(rc)
    print("\n[DONE] All transformation modules completed successfully.")

if __name__ == "__main__":
    main()
