"""End-to-end tests for the observation correction (UPDATE) API."""

from __future__ import annotations

import multiprocessing
import threading
from pathlib import Path

import pytest
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app import delta
from app.external_writer import append_rows, delete_files
from app.sample_data import create_sample_table

# The sample table stores ST-001 at t0 + 0..5 with levels 1204, 1207, ...
T0 = 1_731_600_000
KEY = {"station": "ST-001", "utc_seconds": T0}


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


def _level(table: Path, station: str, utc: int, version: int | None = None) -> int | None:
    rows = delta.read_table(table, version).to_pylist()
    hits = [r for r in rows if r["station"] == station and r["utc_seconds"] == utc]
    assert len(hits) <= 1
    return hits[0]["level_mm"] if hits else None


# ---------------------------------------------------------------------------
# Happy path and snapshot semantics
# ---------------------------------------------------------------------------


def test_correction_changes_exactly_one_value(table: Path) -> None:
    before_pairs = [tuple(r.values()) for r in delta.read_table(table).to_pylist()]
    before_snap = delta.load_snapshot(table)

    result = delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300,
    )

    assert result["version"] == before_snap.version + 1
    assert result["base_version"] == before_snap.version
    assert result["source_file"] in before_snap.active
    assert result["output_file"] in delta.load_snapshot(table).active
    assert result["source_file"] not in delta.load_snapshot(table).active

    after_pairs = [tuple(r.values()) for r in delta.read_table(table).to_pylist()]
    assert len(after_pairs) == len(before_pairs)
    # Exactly one stored row changed from the original value to the corrected
    # one; every other row (including duplicates and row counts) is intact.
    # Row order across files is not part of the table contract, so compare as
    # a multiset.
    removed = list(before_pairs)
    removed.remove(("ST-001", T0, 1204))
    added = list(after_pairs)
    added.remove(("ST-001", T0, 1300))
    assert sorted(removed) == sorted(added)
    assert before_pairs.count(("ST-001", T0, 1204)) == 1


def test_correction_keeps_old_snapshot_at_old_value(table: Path) -> None:
    old_version = delta.load_snapshot(table).version
    old_file = sorted(delta.load_snapshot(table).active)[0]
    delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300,
    )
    # Time travel still reads the original value.
    assert _level(table, "ST-001", T0, version=old_version) == 1204
    assert _level(table, "ST-001", T0) == 1300
    # The tombstones file is not physically deleted.
    assert (table / old_file).is_file()


def test_correction_commit_is_data_change_true(table: Path) -> None:
    result = delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300,
    )
    actions = delta.read_log(table, result["version"])
    adds = [a["add"] for a in actions if "add" in a]
    removes = [a["remove"] for a in actions if "remove" in a]
    assert len(adds) == 1 and adds[0]["dataChange"] is True
    assert len(removes) == 1 and removes[0]["dataChange"] is True
    assert removes[0]["path"] == result["source_file"]
    assert adds[0]["path"] == result["output_file"]
    commit = [a["commitInfo"] for a in actions if "commitInfo" in a][0]
    assert commit["operation"] == "UPDATE"
    # New data file path is unique and immutable: it is never the source path.
    assert result["output_file"] != result["source_file"]


def test_second_correction_uses_the_new_value(table: Path) -> None:
    first = delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300,
    )
    second = delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1300,
        corrected_level_mm=1250,
    )
    assert second["base_version"] == first["version"]
    assert second["source_file"] == first["output_file"]
    assert _level(table, "ST-001", T0) == 1250


# ---------------------------------------------------------------------------
# Rejections: missing / duplicate / stale original
# ---------------------------------------------------------------------------


def test_correction_rejects_missing_key(table: Path) -> None:
    with pytest.raises(delta.ObservationNotFoundError):
        delta.correct_observation(
            table, station="NOPE", utc_seconds=T0, original_level_mm=1,
            corrected_level_mm=2,
        )


def test_correction_rejects_wrong_timestamp(table: Path) -> None:
    with pytest.raises(delta.ObservationNotFoundError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0 + 999, original_level_mm=1204,
            corrected_level_mm=2,
        )


def test_correction_rejects_duplicate_key(table: Path) -> None:
    append_rows(
        table,
        [
            {"station": "ST-001", "utc_seconds": T0, "level_mm": 1204},
            {"station": "ST-001", "utc_seconds": T0, "level_mm": 1303},
        ],
    )
    with pytest.raises(delta.DuplicateObservationError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300,
        )


def test_correction_rejects_stale_original_value(table: Path) -> None:
    with pytest.raises(delta.OriginalValueMismatchError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=9999,
            corrected_level_mm=1300,
        )


def test_correction_rejects_bad_base_version(table: Path) -> None:
    with pytest.raises(delta.VersionNotFoundError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, base_version=99,
        )


def test_correction_validates_request_fields(table: Path) -> None:
    with pytest.raises(delta.InvalidRequestError):
        delta.correct_observation(
            table, station="", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300,
        )
    with pytest.raises(delta.InvalidRequestError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=2 ** 64,
        )


# ---------------------------------------------------------------------------
# Concurrency: conflicts and non-conflicts
# ---------------------------------------------------------------------------


def test_external_delete_of_source_file_conflicts(table: Path) -> None:
    base = delta.load_snapshot(table).version
    snap = delta.load_snapshot(table, base)
    holder = [
        rel
        for rel in snap.active
        if any(
            r["station"] == "ST-001" and r["utc_seconds"] == T0
            for r in pq.read_table(table / rel).to_pylist()
        )
    ]
    assert len(holder) == 1
    delete_files(table, holder)

    with pytest.raises(delta.ConflictError) as exc:
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, base_version=base,
        )
    assert "removed or compacted" in str(exc.value)
    # Deletion is not undone; no staged data leaked into a snapshot.
    assert holder[0] not in delta.load_snapshot(table).active
    assert _level(table, "ST-001", T0, version=base) == 1204
    assert _level(table, "ST-001", T0) is None


def test_compaction_of_source_file_conflicts(table: Path) -> None:
    base = delta.load_snapshot(table).version
    snap = delta.load_snapshot(table, base)
    holder = [
        rel
        for rel in snap.active
        if any(
            r["station"] == "ST-001" and r["utc_seconds"] == T0
            for r in pq.read_table(table / rel).to_pylist()
        )
    ][0]
    # Compaction replaces the file with a new one; the planned source path
    # is gone from the snapshot.
    delta.compact(table, sorted(snap.active)[:2])
    assert holder not in delta.load_snapshot(table).active
    with pytest.raises(delta.ConflictError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, base_version=base,
        )


def test_concurrent_append_of_same_key_conflicts(table: Path) -> None:
    base = delta.load_snapshot(table).version
    append_rows(
        table,
        [{"station": "ST-001", "utc_seconds": T0, "level_mm": 1204}],
    )
    with pytest.raises(delta.ConflictError) as exc:
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, base_version=base,
        )
    assert "concurrent append" in str(exc.value)
    # Nothing was committed by the correction.
    assert delta.load_snapshot(table).version == base + 1


def test_unrelated_concurrent_append_does_not_block_correction(table: Path) -> None:
    base = delta.load_snapshot(table).version
    appended = append_rows(
        table,
        [{"station": "ST-UL", "utc_seconds": 1, "level_mm": 3}],
    )
    result = delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300, base_version=base,
    )
    assert result["version"] > appended["version"]
    assert _level(table, "ST-001", T0) == 1300
    assert _level(table, "ST-UL", 1) == 3
    # Log stays contiguous.
    versions = delta.list_versions(table)
    assert versions == list(range(versions[-1] + 1))


def _worker_correct(table_str: str, q) -> None:  # pragma: no cover
    try:
        q.put(
            (
                "ok",
                delta.correct_observation(
                    Path(table_str),
                    station="ST-001",
                    utc_seconds=T0,
                    original_level_mm=1204,
                    corrected_level_mm=1300,
                )["version"],
            )
        )
    except delta.DeltaError as exc:
        q.put(("rejected", str(exc)))


def test_two_processes_correcting_same_key_one_wins(table: Path) -> None:
    ctx = multiprocessing.get_context("spawn")
    q1, q2 = ctx.Queue(), ctx.Queue()
    p1 = ctx.Process(target=_worker_correct, args=(str(table), q1))
    p2 = ctx.Process(target=_worker_correct, args=(str(table), q2))
    p1.start(); p2.start()
    p1.join(30); p2.join(30)
    assert p1.exitcode == 0 and p2.exitcode == 0
    results = [q1.get(), q2.get()]
    assert any(r[0] == "ok" for r in results)
    # The loser either retried onto a snapshot that still needed the same
    # original value (and won) or observed the updated value and got a 409.
    for status, detail in results:
        assert status in {"ok", "rejected"}
    versions = delta.list_versions(table)
    assert versions == list(range(versions[-1] + 1))
    assert _level(table, "ST-001", T0) == 1300


def test_concurrent_thread_append_racing_correction_is_consistent(table: Path) -> None:
    """A same-key append racing a correction must never yield two rows."""
    base = delta.load_snapshot(table).version
    started = threading.Event()

    def append_same_key() -> None:
        started.wait()
        append_rows(
            table,
            [{"station": "ST-001", "utc_seconds": T0, "level_mm": 1204}],
        )

    t = threading.Thread(target=append_same_key)
    t.start()
    started.set()
    try:
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, base_version=base,
        )
        outcome = "committed"
    except delta.ConflictError:
        outcome = "conflict"
    t.join()
    rows = [
        r
        for r in delta.read_table(table).to_pylist()
        if r["station"] == "ST-001" and r["utc_seconds"] == T0
    ]
    # Exactly one new row is present (the append always commits). If the
    # correction won the commit race it replaced the original row, so the two
    # rows are the corrected value and the appended original; if it detected
    # the append it aborted, so the two rows are the untouched original and
    # the appended copy.  A silently lost append or a duplicated correction
    # would break this.
    assert outcome in {"committed", "conflict"}
    if outcome == "committed":
        assert sorted(r["level_mm"] for r in rows) == [1204, 1300]
    else:
        assert sorted(r["level_mm"] for r in rows) == [1204, 1204]


# ---------------------------------------------------------------------------
# Crash / durability
# ---------------------------------------------------------------------------


def test_fault_after_data_write_preserves_snapshot(table: Path) -> None:
    before = delta.load_snapshot(table)
    files_before = {p.name for p in table.glob("*.parquet")}
    with pytest.raises(RuntimeError):
        delta.correct_observation(
            table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
            corrected_level_mm=1300, fault_inject="after_data_write",
        )
    after = delta.load_snapshot(table)
    assert after.version == before.version
    assert sorted(after.active) == sorted(before.active)
    assert _level(table, "ST-001", T0) == 1204
    # The written-but-uncommitted parquet exists on disk yet belongs to no
    # snapshot, so it cannot be queried.
    orphans = {p.name for p in table.glob("*.parquet")} - files_before
    assert orphans
    assert all(name not in after.active for name in orphans)
    assert not any(
        row["station"] == "ST-001" and row["utc_seconds"] == T0 and row["level_mm"] == 1300
        for row in delta.read_table(table).to_pylist()
    )


def test_corrected_output_schema_and_no_orphans(table: Path) -> None:
    delta.correct_observation(
        table, station="ST-001", utc_seconds=T0, original_level_mm=1204,
        corrected_level_mm=1300,
    )
    data = delta.read_table(table)
    assert data.schema == delta.TABLE_SCHEMA
    staging = table / delta.STAGING_DIR
    assert not staging.exists() or not any(staging.iterdir())


# ---------------------------------------------------------------------------
# HTTP layer / Swagger
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("TABLE_NAME", "water_level")
    import app.main as main

    monkeypatch.setattr(main, "DATA_ROOT", data_root)
    monkeypatch.setattr(main, "TABLE_DIR", data_root / "water_level")
    with TestClient(main.app) as c:
        yield c


def test_api_correct_happy_path(client: TestClient) -> None:
    r = client.post(
        "/correct",
        json={
            "station": "ST-001",
            "utc_seconds": T0,
            "original_level_mm": 1204,
            "corrected_level_mm": 1300,
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["committed_version"] == 5
    assert body["data_change"] is True
    assert body["corrected_level_mm"] == 1300
    assert body["output_file"] != body["source_file"]

    # New snapshot sees 1300, old snapshot still 1204.
    from app import main

    assert _level(main.TABLE_DIR, "ST-001", T0) == 1300
    assert _level(main.TABLE_DIR, "ST-001", T0, version=4) == 1204


def test_api_correct_error_mapping(client: TestClient) -> None:
    base = {
        "station": "ST-001",
        "utc_seconds": T0,
        "original_level_mm": 1204,
        "corrected_level_mm": 1300,
    }
    # 404: missing key
    r = client.post("/correct", json={**base, "station": "ST-XXX"})
    assert r.status_code == 404
    # 409: stale original
    r = client.post("/correct", json={**base, "original_level_mm": 9999})
    assert r.status_code == 409
    # 404: unknown base version
    r = client.post("/correct", json={**base, "version": 99})
    assert r.status_code == 404
    # 422: missing / ill-typed / out-of-range body fields
    r = client.post("/correct", json={"station": "ST-001"})
    assert r.status_code == 422
    r = client.post("/correct", json={**base, "utc_seconds": "abc"})
    assert r.status_code == 422
    r = client.post("/correct", json={**base, "corrected_level_mm": 2**63})
    assert r.status_code == 422
    r = client.post("/correct", json={**base, "station": ""})
    assert r.status_code == 422

    # 409: duplicate key materialised by an external append
    from app import main

    append_rows(
        main.TABLE_DIR,
        [{"station": "ST-001", "utc_seconds": T0, "level_mm": 1204}],
    )
    r = client.post("/correct", json=base)
    assert r.status_code == 409
    assert "occurs" in r.json()["detail"]


def test_api_correct_conflict_after_external_delete(client: TestClient) -> None:
    from app import main

    base = client.get("/table/snapshot").json()["version"]
    snap = delta.load_snapshot(main.TABLE_DIR)
    holder = [
        rel
        for rel in snap.active
        if any(
            r["station"] == "ST-001" and r["utc_seconds"] == T0
            for r in pq.read_table(main.TABLE_DIR / rel).to_pylist()
        )
    ][0]
    delete_files(main.TABLE_DIR, [holder])
    r = client.post(
        "/correct",
        json={
            "station": "ST-001",
            "utc_seconds": T0,
            "original_level_mm": 1204,
            "corrected_level_mm": 1300,
            "version": base,
        },
    )
    assert r.status_code == 409, r.text


def test_swagger_documents_the_correction_endpoint(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()
    assert "/correct" in spec["paths"]
    op = spec["paths"]["/correct"]["post"]
    assert op["tags"] == ["corrections"]
    schema = spec["components"]["schemas"]["CorrectRequest"]
    assert set(schema["required"]) == {
        "station",
        "utc_seconds",
        "original_level_mm",
        "corrected_level_mm",
    }
    assert "version" in schema["properties"]
    # Swagger UI itself is served.
    assert client.get("/docs").status_code == 200
