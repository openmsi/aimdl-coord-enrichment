# Issue 23, Step 1 — girder_io scoped partition helpers

Tracking: https://github.com/openmsi/aimdl-coord-enrichment/issues/23

## Context

Branch: `refactor/issue23-dynamic-partitions`. Step 0 is complete
(branch exists, docs brought forward if needed, baseline tests green).

Before editing, read:

- `.claude/CLAUDE.md`
- `aimdl_coord_enrichment/girder_io.py`
- `tests/test_girder_io.py`

## Why this step

The AIMD-L API exposes partition-aware data types through two
endpoints:

- `GET /aimdl/partition?dataType=<dt>[&since=<iso>]` — returns
  `{"<igsn>//<experiment_date>": "<content_hash>", ...}`, an index
  of partitions for that data_type keyed by content hash.
- `GET /aimdl/partition/details?dataType=<dt>&key=<igsn//experiment_date>`
  — returns the items for exactly one partition, with full meta
  preserved.

Current state in `aimdl_coord_enrichment/girder_io.py`:

- `fetch_partition_keys(client, data_type)` wraps the first endpoint
  but is misnamed (it returns a hash-valued index, not bare keys)
  and does not accept `since`.
- `fetch_items_by_partition(client, data_type)` flattens all
  partitions of a data_type by iterating the index and calling the
  details endpoint inline. It stays as an inventory/reporting helper
  but is no longer suitable for per-partition asset work.
- No scoped single-partition helper exists.

This step adds the scoped helpers. Callers are not rewired in this
step — only the one internal caller inside `fetch_items_by_partition`
uses the new helpers. Asset rewiring lands in Step 2+.

## Goal

In `aimdl_coord_enrichment/girder_io.py`:

- Rename `fetch_partition_keys` → `fetch_partition_index`, adding a
  `since: str | None = None` parameter.
- Add `fetch_partition_details(client, data_type, key) -> list[dict]`.
- Refactor `fetch_items_by_partition` to use both new helpers
  internally, preserving observable behavior.

Update `tests/test_girder_io.py` to match.

## Edits

### 1. `aimdl_coord_enrichment/girder_io.py`

Replace `fetch_partition_keys` with:

```python
def fetch_partition_index(
    client, data_type: str, since: str | None = None
) -> dict[str, str]:
    """Return the partition index for a partition-aware data_type.

    Calls ``GET /aimdl/partition?dataType=<data_type>[&since=<since>]``
    and returns the response dict, keyed by
    ``"<igsn>//<experiment_date>"`` with content-hash values.

    The ``since`` parameter is accepted for future incremental-
    discovery use (e.g. sensor cursors). No caller in this codebase
    wires it up today; pass None to get the full index.
    """
    parameters: dict[str, str] = {"dataType": data_type}
    if since is not None:
        parameters["since"] = since
    return client.get("aimdl/partition", parameters=parameters)
```

Clean rename — no alias left behind. `grep -r fetch_partition_keys`
the repo; there should be no callers outside this file. If any are
found, update them to `fetch_partition_index` (unusual; flag it if so).

Add:

```python
def fetch_partition_details(
    client, data_type: str, key: str
) -> list[dict]:
    """Fetch items for one (data_type, partition-key) pair with full meta.

    Calls ``GET /aimdl/partition/details?dataType=<data_type>&key=<key>``.
    This is the scoped helper used by partition-bound assets. To
    enumerate all partitions of a data_type, prefer
    ``fetch_partition_index`` plus this per key, or the flattening
    ``fetch_items_by_partition`` (inventory/reporting only).

    ``key`` is the literal AIMD-L partition key — the string
    ``"<igsn>//<experiment_date>"`` as emitted by the Girder plugin
    and returned by ``fetch_partition_index``.
    """
    return client.get(
        "aimdl/partition/details",
        parameters={"dataType": data_type, "key": key},
    )
```

Refactor `fetch_items_by_partition` so that:

- Its call to `fetch_partition_keys` becomes `fetch_partition_index`.
- Its inline `client.get("aimdl/partition/details", ...)` becomes a
  call to `fetch_partition_details`.

Preserve all observable behavior — silent skip of empty details,
flat concatenation in iteration order. Update its docstring to note
its demoted role (inventory/reporting, not per-partition assets).

### 2. `tests/test_girder_io.py`

- Update imports at top: replace `fetch_partition_keys` with
  `fetch_partition_index`; add `fetch_partition_details`.

- Rename `test_fetch_partition_keys` → `test_fetch_partition_index`.
  Keep the existing mock-client shape; verify the GET call with
  `parameters={"dataType": "xrd_raw"}` and return-value pass-through.

- Add `test_fetch_partition_index_with_since`:

  ```python
  def test_fetch_partition_index_with_since():
      client = MagicMock()
      client.get.return_value = {}
      fetch_partition_index(
          client, "xrd_raw", since="2026-04-01T00:00:00Z"
      )
      client.get.assert_called_once_with(
          "aimdl/partition",
          parameters={
              "dataType": "xrd_raw",
              "since": "2026-04-01T00:00:00Z",
          },
      )
  ```

- Add `test_fetch_partition_details`:

  ```python
  def test_fetch_partition_details():
      client = MagicMock()
      items = [_make_item("a1.tif", "JHAMAB00001", "xrd_raw")]
      client.get.return_value = items
      result = fetch_partition_details(
          client, "xrd_raw", "JHAMAB00001//2026-04-16",
      )
      client.get.assert_called_once_with(
          "aimdl/partition/details",
          parameters={
              "dataType": "xrd_raw",
              "key": "JHAMAB00001//2026-04-16",
          },
      )
      assert result == items
  ```

- The existing `test_fetch_items_by_partition_paginates_over_keys`
  and `test_fetch_items_by_partition_handles_empty_partition` must
  continue to pass unchanged. The internal refactor of
  `fetch_items_by_partition` is behavior-preserving. If they fail,
  fix the refactor, not the tests.

## Verification

Run the full test suite — not just `test_girder_io.py`. Other tests
should all pass; nothing else in the repo references
`fetch_partition_keys`.

```bash
.venv/bin/pytest
```

If anything outside `tests/test_girder_io.py` fails, grep the repo
for `fetch_partition_keys` and update any stray reference.

## Commit

```
git add aimdl_coord_enrichment/girder_io.py tests/test_girder_io.py
git commit -m "girder_io: add scoped partition helpers (#23)

- Rename fetch_partition_keys -> fetch_partition_index with since= param
- Add fetch_partition_details(data_type, key) for scoped per-partition fetches
- Refactor fetch_items_by_partition internals to use both new helpers"
```

## Success criteria

- `fetch_partition_keys` no longer exists anywhere in the codebase.
- `fetch_partition_index(client, data_type, since=None)` exists and
  works with either arity.
- `fetch_partition_details(client, data_type, key)` exists.
- `fetch_items_by_partition` internally uses both new helpers.
- Full `pytest` suite passes.
- Exactly one new commit.

## Out of scope

- Wiring `since` into any caller. Future optimization.
- Using `fetch_partition_details` in any asset. Steps 2+.
- Any other changes to `girder_io.py`.
