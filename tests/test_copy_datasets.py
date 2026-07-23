"""Unit tests for copy_datasets.py, run against a synthetic project tree.

No real data is touched: a fake project (a few small files with the same
relative paths as the real manifest) is built in a temp dir and copied to
another temp dir.

Run with:  python -m pytest tests/test_copy_datasets.py -q
"""

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import copy_datasets as cd


# --- Synthetic fixtures -------------------------------------------------------
@pytest.fixture
def fake_project(tmp_path):
    """A miniature project tree containing every path in the real manifest."""
    root = tmp_path / "project"
    for _tier, relative, _note in cd.MANIFEST:
        target = root / relative
        if target.suffix:  # file entries
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"x" * 128)
        else:  # directory entries (e.g. the RWI folder)
            target.mkdir(parents=True, exist_ok=True)
            (target / "AAA_relative_wealth_index.csv").write_text("rwi\n1\n")
            (target / "BBB_relative_wealth_index.csv").write_text("rwi\n2\n")
    outputs = root / "Outputs"
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "linear_dummies_true_spi1_avg_stdm_t quarterly standard_fe standard_sym.tex").write_text("tex")
    (outputs / "ignore_me.txt").write_text("not a tex file")
    (outputs / "heterogeneity" / "rural").mkdir(parents=True, exist_ok=True)
    (outputs / "heterogeneity" / "rural" / "table.tex").write_text("tex")
    return root


# --- human_size / path_size ---------------------------------------------------
def test_human_size_units():
    assert cd.human_size(512) == "512 B"
    assert cd.human_size(2048) == "2.0 KB"
    assert cd.human_size(5 * 1024**3) == "5.0 GB"


def test_path_size_file_dir_and_missing(tmp_path):
    f = tmp_path / "a.bin"
    f.write_bytes(b"0" * 10)
    d = tmp_path / "d"
    (d / "sub").mkdir(parents=True)
    (d / "one.txt").write_bytes(b"0" * 3)
    (d / "sub" / "two.txt").write_bytes(b"0" * 4)
    assert cd.path_size(f) == 10
    assert cd.path_size(d) == 7
    assert cd.path_size(tmp_path / "nope") == 0


def test_iter_files_relative_paths(tmp_path):
    d = tmp_path / "folder"
    (d / "sub").mkdir(parents=True)
    (d / "a.csv").write_text("a")
    (d / "sub" / "b.csv").write_text("b")
    relatives = sorted(rel.as_posix() for _abs, rel in cd.iter_files(d))
    assert relatives == ["folder/a.csv", "folder/sub/b.csv"]


# --- build_jobs ---------------------------------------------------------------
def test_build_jobs_respects_tiers(fake_project, tmp_path):
    core = cd.build_jobs(["core"], project_root=fake_project, dest_root=tmp_path / "dest")
    assert core and all(job["tier"] == "core" for job in core)
    assert all(job["exists"] for job in core)
    assert len(core) == sum(1 for tier, _r, _n in cd.MANIFEST if tier == "core")


def test_build_jobs_dest_mirrors_relative_path(fake_project, tmp_path):
    dest = tmp_path / "dest"
    for job in cd.build_jobs(["core"], project_root=fake_project, dest_root=dest):
        assert job["dest"] == dest / job["relative"]


def test_build_jobs_flags_missing_sources(fake_project, tmp_path):
    victim = fake_project / cd.MANIFEST[0][1]
    victim.unlink()
    jobs = cd.build_jobs(["core"], project_root=fake_project, dest_root=tmp_path / "dest")
    missing = [job for job in jobs if not job["exists"]]
    assert len(missing) == 1
    assert missing[0]["relative"].as_posix() == cd.MANIFEST[0][1]


def test_build_jobs_outputs_tier_globs_only_tex(fake_project, tmp_path):
    jobs = cd.build_jobs(["outputs"], project_root=fake_project, dest_root=tmp_path / "dest")
    names = {job["relative"].name for job in jobs}
    assert "ignore_me.txt" not in names
    assert "table.tex" in names
    assert any(name.endswith("standard_sym.tex") for name in names)


def test_build_jobs_external_env_var_unset(fake_project, tmp_path, monkeypatch):
    monkeypatch.delenv("CLIMATE_BANDS", raising=False)
    jobs = cd.build_jobs(["upstream"], project_root=fake_project, dest_root=tmp_path / "dest")
    external = [job for job in jobs if "CLIMATE_BANDS" in job["note"]]
    assert len(external) == 1
    assert external[0]["exists"] is False


def test_build_jobs_external_env_var_set(fake_project, tmp_path, monkeypatch):
    grd = tmp_path / "koppen.grd"
    grd.write_bytes(b"grid")
    monkeypatch.setenv("CLIMATE_BANDS", str(grd))
    jobs = cd.build_jobs(["upstream"], project_root=fake_project, dest_root=tmp_path / "dest")
    external = [job for job in jobs if job["source"] == grd]
    assert len(external) == 1
    assert external[0]["exists"] is True
    assert external[0]["relative"].as_posix() == "Data/External/CLIMATE_BANDS/koppen.grd"


# --- copying ------------------------------------------------------------------
def test_needs_copy_logic(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"0" * 10)
    dst = tmp_path / "dst.bin"
    assert cd.needs_copy(src, dst) is True
    shutil.copy2(src, dst)
    assert cd.needs_copy(src, dst) is False
    dst.write_bytes(b"0" * 11)
    assert cd.needs_copy(src, dst) is True


def test_copy_one_creates_parents(tmp_path):
    src = tmp_path / "src.bin"
    src.write_bytes(b"0" * 20)
    dst = tmp_path / "a" / "b" / "c" / "src.bin"
    assert cd.copy_one(src, dst) == 20
    assert dst.read_bytes() == b"0" * 20
    # second call is a no-op
    assert cd.copy_one(src, dst) == 0


def test_copy_job_directory_mirrors_tree(fake_project, tmp_path):
    dest = tmp_path / "dest"
    jobs = cd.build_jobs(["upstream"], project_root=fake_project, dest_root=dest)
    dir_jobs = [job for job in jobs if job["source"] is not None and Path(job["source"]).is_dir()]
    assert dir_jobs, "expected at least one directory entry (RWI)"
    for job in dir_jobs:
        cd.copy_job(job)
        for file_path, _rel in cd.iter_files(job["source"]):
            mirrored = Path(job["dest"]) / file_path.relative_to(job["source"])
            assert mirrored.is_file()


def test_run_copies_core_tier_and_leaves_source_intact(fake_project, tmp_path):
    dest = tmp_path / "dest"
    before = sorted(p.relative_to(fake_project).as_posix() for p in fake_project.rglob("*") if p.is_file())
    copied = cd.run(dest, ["core"], project_root=fake_project)
    assert copied > 0
    for _tier, relative, _note in cd.MANIFEST:
        if _tier != "core":
            continue
        assert (dest / relative).exists()
    # nothing outside the core tier was copied
    assert not (dest / "Data/Data_out/Climate_shocks_v11.nc").exists()
    # the source tree is untouched (copy, never move)
    after = sorted(p.relative_to(fake_project).as_posix() for p in fake_project.rglob("*") if p.is_file())
    assert before == after


def test_run_dry_run_copies_nothing(fake_project, tmp_path):
    dest = tmp_path / "dest"
    assert cd.run(dest, ["core", "resume"], dry_run=True, project_root=fake_project) == 0
    assert not dest.exists()


def test_run_is_idempotent(fake_project, tmp_path):
    dest = tmp_path / "dest"
    first = cd.run(dest, ["core"], project_root=fake_project)
    second = cd.run(dest, ["core"], project_root=fake_project)
    assert first > 0
    assert second == 0  # everything already present with matching sizes


def test_parse_args_defaults():
    args = cd.parse_args(["--dest", "/somewhere"])
    assert args.tier == cd.DEFAULT_TIERS
    assert args.dry_run is False
    args = cd.parse_args(["--dest", "/somewhere", "--tier", "core", "upstream", "--dry-run"])
    assert args.tier == ["core", "upstream"]
    assert args.dry_run is True
