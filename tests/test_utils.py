"""Unit tests for utils.py (quiet netCDF opening helpers).

These use only synthetic data / subprocesses -- no project Data/ is touched.
The key behaviour under test is that C-level stderr noise (as emitted by
libnetcdf 4.9.3's unquoted ``getfattr`` DAOS probe) is captured, while data
still loads correctly and Python exceptions still propagate.

Run:  pytest tests/test_utils.py -q
"""

import os
import sys
import subprocess
from pathlib import Path

import pytest

# Scripts live in the project root; make utils importable.
sys.path.insert(0, str(Path(__file__).parent.parent))
import utils  # noqa: E402


def _capture_fd2(func, sink_path):
    """Run ``func()`` with the process's real fd 2 pointed at ``sink_path``.

    Returns the text written to fd 2 during the call. This lets us observe
    exactly what escapes to the OS-level stderr, independent of pytest's own
    capture machinery.
    """
    saved = os.dup(2)
    fd = os.open(str(sink_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        os.dup2(fd, 2)
        os.close(fd)
        func()
    finally:
        os.dup2(saved, 2)
        os.close(saved)
    return Path(sink_path).read_text()


def test_suppress_hides_fd2_and_subprocess_stderr(tmp_path):
    """Writes to fd 2 inside the block -- direct and from a child process --
    must not escape, while writes outside it must."""
    sink = tmp_path / "captured.txt"

    def scenario():
        os.write(2, b"BEFORE\n")
        with utils.suppress_c_stderr():
            os.write(2, b"HIDDEN-DIRECT\n")
            # A child process inherits fd 2 == /dev/null inside the block,
            # mirroring how libnetcdf's getfattr subprocess emits its noise.
            subprocess.run(
                [sys.executable, "-c",
                 "import sys; sys.stderr.write('HIDDEN-SUBPROC\\n'); sys.stderr.flush()"],
                check=True,
            )
        os.write(2, b"AFTER\n")

    text = _capture_fd2(scenario, sink)
    assert "BEFORE" in text
    assert "AFTER" in text  # fd 2 was restored after the block
    assert "HIDDEN-DIRECT" not in text
    assert "HIDDEN-SUBPROC" not in text


def test_suppress_restores_fd2_on_exception(tmp_path):
    """An exception inside the block still propagates, and fd 2 is restored."""
    sink = tmp_path / "captured.txt"

    def scenario():
        with pytest.raises(ValueError, match="boom"):
            with utils.suppress_c_stderr():
                raise ValueError("boom")
        os.write(2, b"STILL-WORKS\n")  # fd 2 usable again

    text = _capture_fd2(scenario, sink)
    assert "STILL-WORKS" in text


def test_open_dataset_roundtrip_and_quiet_on_spaced_path(tmp_path):
    """open_dataset loads synthetic data correctly, even from a path with
    spaces (the exact condition that triggers the getfattr flood), and emits
    no ``getfattr`` noise to fd 2."""
    xr = pytest.importorskip("xarray")
    import numpy as np

    # A directory with a space reproduces the unquoted-path split condition.
    spaced_dir = tmp_path / "a b"
    spaced_dir.mkdir()
    nc_path = spaced_dir / "synthetic.nc"

    ds = xr.Dataset(
        {"t": ("x", np.arange(5.0))},
        coords={"x": np.arange(5)},
    )
    ds.to_netcdf(nc_path)
    ds.close()

    sink = tmp_path / "captured.txt"
    result = {}

    def scenario():
        out = utils.open_dataset(nc_path)
        try:
            result["values"] = out["t"].values.tolist()
        finally:
            out.close()

    text = _capture_fd2(scenario, sink)
    assert result["values"] == [0.0, 1.0, 2.0, 3.0, 4.0]
    # Whether or not getfattr is installed, none of its noise should escape.
    assert "getfattr" not in text
