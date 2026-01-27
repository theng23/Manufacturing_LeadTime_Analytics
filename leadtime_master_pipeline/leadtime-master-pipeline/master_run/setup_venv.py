"""Setup .venv and install dependencies."""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENV = ROOT / ".venv"

def run(cmd):
    print("[CMD]", " ".join(map(str, cmd)))
    subprocess.check_call(list(map(str, cmd)))

def main():
    if not VENV.exists():
        run([sys.executable, "-m", "venv", str(VENV)])

    pip = VENV / ("Scripts/pip.exe" if sys.platform.startswith("win") else "bin/pip")
    run([str(pip), "install", "--upgrade", "pip"])
    run([str(pip), "install", "-r", str(ROOT / "requirements.txt")])

if __name__ == "__main__":
    main()
