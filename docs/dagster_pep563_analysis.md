# Dagster + PEP 563 `from __future__ import annotations` — Analysis

**Date:** 2026-04-20
**Dagster versions tested:** 1.12.22, 1.13.1
**Python:** 3.12.9
**Reproduces on both versions identically.**

---

## 1. The symptom

Any module that combines `from __future__ import annotations` with a Dagster
`@asset` (or `@op`) whose signature includes a `config: MyConfig` parameter
fails at import time:

```
DagsterInvalidPythonicConfigDefinitionError:
Unable to resolve config type 'MyConfig' to a supported Dagster config type.
```

Minimal reproduction (9 lines, no imports from this project):

```python
from __future__ import annotations
from dagster import asset, AssetExecutionContext, Config

class MyConfig(Config):
    dry_run: bool = True

@asset
def my_asset(context: AssetExecutionContext, config: MyConfig) -> dict:
    return {}
```

This fails even when the Config class is defined in the same file, has no
`Optional` fields, no forward references, and no nesting.

---

## 2. Root cause

PEP 563 (`from __future__ import annotations`) turns all annotations into
**strings** at parse time. Under PEP 563, `my_asset.__annotations__` contains
`{'config': 'MyConfig', ...}` rather than `{'config': <class MyConfig>, ...}`.

Python provides `typing.get_type_hints(fn)` to resolve these strings back to
the actual types using the function's module globals. This is the standard
mechanism that PEP 563 was designed to work with.

### What Dagster fixed (PR #14707, July 2023)

Issue [#14571](https://github.com/dagster-io/dagster/issues/14571) addressed
PEP 563 for **Config class field annotations** — the types declared *inside*
a `dagster.Config` subclass (e.g., `Optional[str]`, `list[int]`). That fix
uses `typing.get_type_hints()` on the Config class to resolve its Pydantic
field types. This works correctly.

### What remains broken

The **`@asset` / `@op` decorator** reads the `config` parameter's type
annotation from `inspect.signature(fn)`, not from `typing.get_type_hints(fn)`.
Under PEP 563, `inspect.signature` preserves the raw string annotation.

The code path (in `dagster/_core/definitions/decorators/op_decorator.py`,
`_Op.__call__`):

```python
config_arg = compute_fn.get_config_arg()
config_arg_type = config_arg.annotation      # ← string under PEP 563
config_arg_default = config_arg.default
self.config_schema = infer_schema_from_config_annotation(
    config_arg_type, config_arg_default       # ← receives a string
)
```

`infer_schema_from_config_annotation` then calls
`safe_is_subclass(model_cls, Config)`. Since `model_cls` is the string
`'MyConfig'` rather than the class, `safe_is_subclass` returns `False`,
and the function falls through to a code path that tries to interpret it
as a primitive Dagster config type — which fails.

### Verified fix

Replacing `config_arg.annotation` with the resolved type from
`typing.get_type_hints(fn)` fixes the issue:

```python
>>> import typing
>>> hints = typing.get_type_hints(my_asset)
>>> hints['config']
<class '__main__.MyConfig'>
>>> safe_is_subclass(hints['config'], Config)
True
```

The same gap likely exists for `context` and resource parameter annotations,
but those happen to be handled by different code paths that may or may not
also break. We have not investigated those.

---

## 3. Scope of impact

The bug affects **any module** that:

1. Uses `from __future__ import annotations`, AND
2. Defines a function decorated with `@asset`, `@op`, or `@multi_asset`, AND
3. That function has a `config: SomeConfigSubclass` parameter

It does **not** affect:

- Modules that define Config classes but no `@asset`/`@op` functions
- Modules that define `@asset` functions without a `config` parameter
- Modules that use `@asset` with `config` but omit `from __future__ import annotations`
- Resource annotations (separate resolution path, apparently uses `get_type_hints`)

---

## 4. Options

### Option A: Omit PEP 563 in asset modules (current approach)

Remove `from __future__ import annotations` from any `.py` file that
defines `@asset` or `@op` functions with a `config` parameter. Other
modules (pure logic, helpers, type definitions) can continue using it.

**Pros:** Zero risk, no dependency on upstream fix, no monkey-patching.
**Cons:** Inconsistent import style across the codebase. Easy to forget
and get a confusing error. Requires a comment or convention doc explaining
why.

**Files currently affected in this project:**
- `helix_dagster/coord_enrichment/enrichment_leaves.py` (already omits it)
- `helix_dagster/coord_enrichment/provenance_tagging.py` (already omits it)
- `helix_dagster/assets.py` (already omits it)

### Option B: File an upstream issue

The reproduction is trivial and the fix is small — `_Op.__call__` needs to
resolve string annotations via `typing.get_type_hints(fn)` before passing
them to `infer_schema_from_config_annotation`. This is the same approach
PR #14707 applied to Config field annotations.

A similar resolution would be needed anywhere else `inspect.signature` is
used to extract annotation types: `get_resource_args()`, `get_config_arg()`,
`has_config_arg()`, and the `validate_resource_annotated_function` call.

**Pros:** Fixes the problem for all Dagster users. Aligns with the intent
of PR #14707.
**Cons:** Depends on upstream prioritization and release timing. We still
need Option A in the interim.

### Option C: Local monkey-patch

Patch `_Op.__call__` at import time to resolve annotations before Dagster
processes them. This would live in our `__init__.py` or a conftest.

**Not recommended.** Brittle across Dagster upgrades, hard to maintain,
and the workaround (Option A) is simpler.

### Option D: Use `Annotated` type aliases to avoid the string annotation

```python
from dagster import Config
from typing import Annotated

class MyConfig(Config):
    dry_run: bool = True

CoordConfig = MyConfig  # concrete alias, not a string under PEP 563
```

This doesn't actually help — under PEP 563 the alias name itself becomes
a string. Only removing the future import avoids the problem.

---

## 5. Recommendation

**Use Option A (omit PEP 563 in asset modules) and file Option B upstream.**

The workaround is low-cost and reliable. The upstream fix is straightforward
and worth contributing. The two are complementary: Option A keeps us working
now, and Option B eliminates the inconsistency in a future Dagster release.

Convention for this project: any module under `helix_dagster/` that defines
`@asset`, `@op`, or `@multi_asset` decorated functions must not use
`from __future__ import annotations`. All other modules may use it freely.
