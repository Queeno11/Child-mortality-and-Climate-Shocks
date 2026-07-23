"""Shared utilities for the Child Mortality & Climate Shocks project.

Small, dependency-light helpers used across the analysis scripts. Add new
general-purpose helpers here rather than duplicating them per script.

Currently provides quiet netCDF opening (see below).

Quiet netCDF opening
--------------------
netCDF-C 4.9.3 (the C library under xarray's default ``netcdf4`` engine)
probes whether a file lives on a DAOS filesystem by shelling out to::

    getfattr <path> | grep -c '.daos'

with ``<path>`` interpolated **unquoted**. When the project sits under a path
containing spaces (e.g. ".../Working Papers/Paper - Child .../"), the shell
splits the path on spaces and ``getfattr`` prints a burst of harmless::

    getfattr: <fragment>: No such file or directory

lines to stderr. The probe still correctly concludes "not DAOS" and the file
opens normally -- the messages are pure cosmetic noise. The bug is fixed in
libnetcdf >= 4.10, but upgrading there drags the whole pinned HDF5 stack
(netcdf4/eccodes hard-pin libnetcdf 4.9.3, whose 4.10 builds pull hdf5 2.x),
so we suppress the noise at the call site instead.

``suppress_c_stderr`` redirects the process's OS-level stderr (file
descriptor 2) to ``os.devnull`` for the duration of a block, capturing the
subprocess' inherited stderr. Python-level exceptions are unaffected -- they
are not written to fd 2 -- so a genuine failure to open still raises normally.

Usage
-----
    import utils
    ds = utils.open_dataset("some_file.nc")         # drop-in for xr.open_dataset

    # or, to wrap an arbitrary block that emits C-level stderr noise:
    with utils.suppress_c_stderr():
        ...
"""

from contextlib import contextmanager
import os
import sys


@contextmanager
def suppress_c_stderr():
    """Silence writes to the OS-level stderr (fd 2) within the block.

    Flushes Python's ``sys.stderr`` buffer first so pending Python output is
    not lost, dup2's ``os.devnull`` over fd 2, and restores the original fd on
    exit -- even if the body raises.
    """
    sys.stderr.flush()
    saved_stderr_fd = os.dup(2)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull_fd, 2)
        yield
    finally:
        os.dup2(saved_stderr_fd, 2)
        os.close(devnull_fd)
        os.close(saved_stderr_fd)


def open_dataset(*args, **kwargs):
    """``xarray.open_dataset`` with the netCDF DAOS-probe noise suppressed.

    A thin wrapper: opens the dataset inside :func:`suppress_c_stderr` so the
    unquoted ``getfattr`` probe in libnetcdf 4.9.3 cannot spam stderr. All
    positional and keyword arguments are forwarded unchanged. ``xarray`` is
    imported lazily so this module stays cheap to import for callers that only
    need :func:`suppress_c_stderr`.
    """
    import xarray as xr

    with suppress_c_stderr():
        return xr.open_dataset(*args, **kwargs)


def open_mfdataset(*args, **kwargs):
    """``xarray.open_mfdataset`` with the netCDF DAOS-probe noise suppressed.

    See :func:`open_dataset`. The suppression covers the file opening done
    eagerly within the call; with ``parallel=True`` on a *process*-based dask
    scheduler, opens executed in separate worker processes fall outside this
    process's fd redirect and are not affected.
    """
    import xarray as xr

    with suppress_c_stderr():
        return xr.open_mfdataset(*args, **kwargs)
