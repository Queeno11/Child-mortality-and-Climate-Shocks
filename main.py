"""Replication pipeline runner.

Sequentially runs the numbered analysis scripts, starting from
02_assign_shocks_to_DHS.py. The earlier scripts (00*, 01*) query and compile
the raw ERA5/CCKP/CRU climate data and take very long to run; they are kept
out of this pipeline and the resulting dataset is distributed as-is with the
replication package.

Usage:
    python main.py

The pipeline halts on the first script that fails.
"""

import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.resolve()

# Ordered list of scripts to run. Julia scripts are run with `julia --project=.`
# so they pick up the Project.toml environment in the project root.
PIPELINE = [
    # "02_assign_shocks_to_DHS.py",
    # "02b_assign_climatic_bands.py",
    "03_merge_climate_and_DHS.py",
    "04_regressions.jl",
    "06_charts.py",
]


def build_command(script_path, python=None, julia="julia"):
    """Return the command (list of str) to execute a pipeline script."""
    script_path = Path(script_path)
    suffix = script_path.suffix.lower()
    if suffix == ".py":
        return [python or sys.executable, str(script_path)]
    if suffix == ".jl":
        return [julia, f"--project={script_path.parent}", str(script_path)]
    raise ValueError(f"Don't know how to run '{script_path.name}' (unsupported extension '{suffix}')")


def run_script(script_path, cwd=None):
    """Run one script, streaming its output. Return True if it succeeded."""
    script_path = Path(script_path)
    command = build_command(script_path)
    print(f"\n{'=' * 70}\n>>> Running {script_path.name}\n{'=' * 70}", flush=True)
    start = time.time()
    result = subprocess.run(command, cwd=cwd or script_path.parent)
    elapsed = time.time() - start
    if result.returncode == 0:
        print(f">>> {script_path.name} finished successfully in {elapsed / 60:.1f} min.", flush=True)
        return True
    print(f"!!! {script_path.name} FAILED with exit code {result.returncode} after {elapsed / 60:.1f} min.", flush=True)
    return False


def run_pipeline(scripts, project_dir=PROJECT_DIR):
    """Run scripts in order, halting on the first failure. Return True if all passed."""
    missing = [name for name in scripts if not (project_dir / name).exists()]
    if missing:
        print(f"ERROR: script(s) not found in {project_dir}: {', '.join(missing)}")
        return False

    for name in scripts:
        if not run_script(project_dir / name, cwd=project_dir):
            print(f"\nPipeline halted at '{name}'. Fix the error above and re-run.")
            return False

    print("\n=== All pipeline scripts completed successfully! ===")
    return True


if __name__ == "__main__":
    sys.exit(0 if run_pipeline(PIPELINE) else 1)
