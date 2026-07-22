"""Unit tests for main.py using synthetic scripts (no real data).

Run with: python test_main.py
"""

import sys
import tempfile
from pathlib import Path

import main


def test_build_command():
    cmd = main.build_command(Path("/proj/02_foo.py"))
    assert cmd == [sys.executable, "/proj/02_foo.py"], cmd

    cmd = main.build_command(Path("/proj/04_regs.jl"))
    assert cmd == ["julia", "--project=/proj", "/proj/04_regs.jl"], cmd

    try:
        main.build_command(Path("/proj/foo.do"))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unsupported extension")
    print("test_build_command passed")


def make_script(dirpath, name, body):
    path = Path(dirpath) / name
    path.write_text(body)
    return path


def test_run_script_success_and_failure():
    with tempfile.TemporaryDirectory() as d:
        ok = make_script(d, "ok.py", "print('hello from ok')\n")
        bad = make_script(d, "bad.py", "import sys; sys.exit(3)\n")
        assert main.run_script(ok) is True
        assert main.run_script(bad) is False
    print("test_run_script_success_and_failure passed")


def test_run_pipeline_order_and_halt():
    with tempfile.TemporaryDirectory() as d:
        log = Path(d) / "order.log"
        make_script(d, "01_a.py", f"open(r'{log}', 'a').write('a\\n')\n")
        make_script(d, "02_b.py", f"open(r'{log}', 'a').write('b\\n'); raise SystemExit(1)\n")
        make_script(d, "03_c.py", f"open(r'{log}', 'a').write('c\\n')\n")

        result = main.run_pipeline(["01_a.py", "02_b.py", "03_c.py"], project_dir=Path(d))
        assert result is False
        # a and b ran in order; c never ran because the pipeline halted at b
        assert log.read_text() == "a\nb\n", log.read_text()

        log.unlink()
        result = main.run_pipeline(["01_a.py", "03_c.py"], project_dir=Path(d))
        assert result is True
        assert log.read_text() == "a\nc\n", log.read_text()
    print("test_run_pipeline_order_and_halt passed")


def test_run_pipeline_missing_script():
    with tempfile.TemporaryDirectory() as d:
        make_script(d, "01_a.py", "pass\n")
        result = main.run_pipeline(["01_a.py", "nope.py"], project_dir=Path(d))
        assert result is False
    print("test_run_pipeline_missing_script passed")


def test_real_pipeline_scripts_exist():
    missing = [s for s in main.PIPELINE if not (main.PROJECT_DIR / s).exists()]
    assert not missing, f"missing scripts: {missing}"
    print("test_real_pipeline_scripts_exist passed")


if __name__ == "__main__":
    test_build_command()
    test_run_script_success_and_failure()
    test_run_pipeline_order_and_halt()
    test_run_pipeline_missing_script()
    test_real_pipeline_scripts_exist()
    print("\nAll tests passed.")
