# Developer note — `from __future__ import annotations`

## Rule

Use `from __future__ import annotations` **only** in pure helper
modules. Do **not** use it in:

- modules that define Dagster assets (`@asset`, `@multi_asset`, etc.)
- modules that define Dagster sensors (`@sensor`)
- modules that define Dagster resources (`ConfigurableResource`,
  `IOManager`, etc.)
- modules that define `dagster.Config` subclasses
- modules that define Pydantic `BaseModel` subclasses consumed by
  Dagster
- the module that registers `Definitions` (currently
  `helix_dagster/__init__.py`)

If a module is on the list above, write annotations using the
concrete typing forms that evaluate at definition time
(`Optional[str]`, `List[int]`, `Dict[str, Any]`). Modern
union syntax like `str | None` is fine; the specific gotcha is
the postponed-evaluation import, not the newer syntax.

## Why

PEP 563 (`from __future__ import annotations`) stores annotations
as strings and defers their evaluation. For most Python code this
is fine — annotations are a static concern only.

Dagster is different. When Dagster builds the config schema for a
job, it inspects `Config` subclass annotations via
`typing.get_type_hints()`. That call resolves the string
annotations against the defining module's namespace — but in
practice the resolution happens at a point where some imports
(particularly forward references or conditional imports) are not
yet available in that namespace. The result is a `NameError` or
`RuntimeError` that surfaces when the Definitions object is
loaded, with a traceback that points nowhere useful.

Concrete example of what breaks: if `CoordEnrichmentConfig`
lives in a module that starts with
`from __future__ import annotations`, then
`manifest_tracking_item_id: Optional[str] = None` becomes the
string `"Optional[str]"`, and Dagster's schema builder can fail
to resolve `Optional` in that context. Without the future import,
the annotation is the actual `Optional[str]` object and nothing
has to be resolved.

Same failure mode applies to resources and any other Pydantic-
backed type the Dagster runtime introspects.

## Scope

This project is Python 3.12+. All the modern typing forms
(`list[int]`, `dict[str, Any]`, `X | None`) work natively
without the future import. The rule costs nothing; it just says
"don't reach for the convenience that breaks the framework."

## Enforcement

A test at `tests/test_annotations_rule.py` scans the
Dagster-adjacent modules and fails if the forbidden import is
present. The failure message names the file and points back to
this note.

If you need to add a new module that defines Dagster assets,
sensors, resources, or `Config` subclasses, add its path to the
`FORBIDDEN_PATHS` list in that test file.

## Allowed modules (examples)

Pure helper modules may use the future import freely:

- `helix_dagster/coordinates.py`
- `helix_dagster/provenance.py`
- `helix_dagster/girder_io.py`
- `helix_dagster/matching.py`
- `helix_dagster/validation.py`
- `helix_dagster/instruments/helix.py`
- `helix_dagster/instruments/maxima.py`
- `helix_dagster/instruments/types.py`
- `helix_dagster/coord_enrichment/overwrite.py`
- `helix_dagster/coord_enrichment/cache.py`

## Forbidden modules (examples)

These define Dagster assets, resources, or Config subclasses:

- `helix_dagster/__init__.py`
- `helix_dagster/assets.py`
- `helix_dagster/checks.py`
- `helix_dagster/resources.py`
- `helix_dagster/sensors.py`
- `helix_dagster/instruments/__init__.py`
- `helix_dagster/coord_enrichment/__init__.py`
- `helix_dagster/coord_enrichment/config.py`
- `helix_dagster/coord_enrichment/config_snapshot.py`
- `helix_dagster/coord_enrichment/inventory.py`
- `helix_dagster/coord_enrichment/provenance_tagging.py`
- `helix_dagster/coord_enrichment/enrichment_leaves.py`
- `helix_dagster/coord_enrichment/report.py`
- `helix_dagster/coord_enrichment/manifest.py`
