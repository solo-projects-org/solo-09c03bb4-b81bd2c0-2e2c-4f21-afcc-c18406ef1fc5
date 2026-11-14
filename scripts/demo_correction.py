"""Demo 4 - correct one observation and prove the old snapshot is untouched.

Drives the correction through the HTTP API:

* a valid correction commits with dataChange=true and changes exactly one row
* the previous snapshot still returns the original level (time travel)
* a stale "original level" and a missing key are rejected (409 / 404)
* an external writer appending an unrelated row does not block the correction
* an external writer appending the *same* key in flight makes it conflict
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, API_2, banner, api, show_snapshot  # noqa: E402

# Sample data seeded by the API on first start.
STATION = "ST-001"
UTC_SECONDS = 1_731_600_000  # first observation of ST-001
ORIGINAL = 1204
CORRECTED = 1300


def _levels(client, version: int | None = None) -> dict[tuple[str, int], int]:
    """Read the table through the Delta log of the running services."""
    from app import delta
    from scripts._demo_common import TABLE_DIR

    snap = delta.load_snapshot(TABLE_DIR, version)
    levels: dict[tuple[str, int], int] = {}
    for row in delta.read_table(TABLE_DIR, snap.version).to_pylist():
        key = (row["station"], row["utc_seconds"])
        levels[key] = levels.get(key, row["level_mm"])
    return levels


def main() -> None:
    client = api(API_1)
    banner("Demo 4: correct one observation, keep the old snapshot readable")

    before = show_snapshot(client)
    before_levels = _levels(client)

    payload = {
        "station": STATION,
        "utc_seconds": UTC_SECONDS,
        "original_level_mm": ORIGINAL,
        "corrected_level_mm": CORRECTED,
    }
    r = client.post("/correct", json=payload)
    print(f"\nPOST /correct -> HTTP {r.status_code}: {r.json()}")
    r.raise_for_status()
    body = r.json()
    assert body["data_change"] is True
    assert body["corrected_level_mm"] == CORRECTED

    after = show_snapshot(client)
    after_levels = _levels(client)
    assert after_levels[(STATION, UTC_SECONDS)] == CORRECTED
    assert after["row_count"] == before["row_count"]
    changed = {
        k for k in before_levels if before_levels.get(k) != after_levels.get(k)
    }
    assert changed == {(STATION, UTC_SECONDS)}, changed
    print(f"\n=> exactly one value changed: {changed.pop()} {ORIGINAL} -> {CORRECTED}")

    banner("Time travel: the pre-correction snapshot keeps the original value")
    old_levels = _levels(client, version=body["base_version"])
    assert old_levels[(STATION, UTC_SECONDS)] == ORIGINAL
    print(f"v{body['base_version']}: {STATION} @ {UTC_SECONDS} = {ORIGINAL} (unchanged)")

    banner("Rejections")
    r = client.post("/correct", json={**payload, "original_level_mm": 9999})
    print(f"stale original value -> HTTP {r.status_code}: {r.json()['detail']}")
    assert r.status_code == 409
    r = client.post("/correct", json={**payload, "station": "ST-NONE"})
    print(f"missing key          -> HTTP {r.status_code}: {r.json()['detail']}")
    assert r.status_code == 404

    banner("Unrelated concurrent append does not block the correction")
    # Move the trickier race exercises to the direct table path, which is the
    # same code the API runs; HTTP is already proven above.
    import multiprocessing
    import time

    from app import delta
    from app.external_writer import append_rows
    from scripts._demo_common import TABLE_DIR

    ctx = multiprocessing.get_context("spawn")
    writer = ctx.Process(
        target=_append_worker,
        args=(str(TABLE_DIR), "ST-EXT", 7_000_000, 5),
    )
    writer.start()
    time.sleep(0.05)
    result = delta.correct_observation(
        Path(TABLE_DIR),
        station="ST-002",
        utc_seconds=1_731_600_000,
        original_level_mm=980,
        corrected_level_mm=981,
    )
    writer.join(30)
    assert writer.exitcode == 0
    print(
        f"correction of ST-002 committed at v{result['version']} while "
        f"{5} unrelated appends landed"
    )
    versions = client.get("/table/versions").json()["versions"]
    assert versions == list(range(versions[-1] + 1)), "log must stay contiguous"
    print(f"log still contiguous from 0: 0..{versions[-1]}")

    banner("Same-key concurrent append makes the correction conflict")
    base = delta.load_snapshot(TABLE_DIR).version
    append_rows(
        Path(TABLE_DIR),
        [{"station": "ST-003", "utc_seconds": 1_731_600_001, "level_mm": 1505}],
    )
    try:
        delta.correct_observation(
            Path(TABLE_DIR),
            station="ST-003",
            utc_seconds=1_731_600_001,
            original_level_mm=1505,
            corrected_level_mm=1506,
            base_version=base,
        )
        raise AssertionError("expected a conflict")
    except delta.ConflictError as exc:
        print(f"correction -> conflict: {exc}")

    print(f"\nreplica #2 ({API_2}) sees latest v"
          f"{api(API_2).get('/table/versions').json()['latest_version']}")
    print("\nOK: corrections are point updates with dataChange=true, old "
          "snapshots stay readable, and conflicts are rejected.")


def _append_worker(table_dir: str, station: str, t0: int, rounds: int) -> None:  # pragma: no cover
    import time
    from pathlib import Path

    from app.external_writer import append_rows

    for i in range(rounds):
        append_rows(
            Path(table_dir),
            [{"station": station, "utc_seconds": t0 + i, "level_mm": 100 + i}],
        )
        time.sleep(0.03)


if __name__ == "__main__":
    main()
