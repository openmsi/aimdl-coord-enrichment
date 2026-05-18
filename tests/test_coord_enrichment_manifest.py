"""Tests for the coord_enrichment_manifest env-var fallback."""

from unittest.mock import MagicMock

import pytest
from dagster import build_asset_context

from aimdl_coord_enrichment.coord_enrichment.config import CoordEnrichmentConfig
from aimdl_coord_enrichment.coord_enrichment.manifest import coord_enrichment_manifest


def _make_girder_mock():
    mock = MagicMock()
    mock.addMetadataToItem = MagicMock()
    return mock


def _make_report():
    return {"leaves": [], "summary": {"total": 0}}


def test_manifest_uses_config_value_when_set(monkeypatch):
    """Config value takes precedence over env var."""
    monkeypatch.setenv("COORD_ENRICHMENT_MANIFEST_ITEM", "item_env")
    config = CoordEnrichmentConfig(
        dry_run=False, manifest_tracking_item_id="item_config",
    )
    girder = _make_girder_mock()
    ctx = build_asset_context()

    result = coord_enrichment_manifest(ctx, config, _make_report(), girder)

    girder.addMetadataToItem.assert_called_once()
    call_args = girder.addMetadataToItem.call_args[0]
    assert call_args[0] == "item_config"
    assert "write_skipped" not in result
    metadata = ctx.get_output_metadata("result")
    assert metadata["tracking_item_source"].value == "config"


def test_manifest_falls_back_to_env_var_when_config_unset(monkeypatch):
    """When config is None, the asset reads the env var."""
    monkeypatch.setenv("COORD_ENRICHMENT_MANIFEST_ITEM", "item_env")
    config = CoordEnrichmentConfig(dry_run=False)
    girder = _make_girder_mock()
    ctx = build_asset_context()

    result = coord_enrichment_manifest(ctx, config, _make_report(), girder)

    girder.addMetadataToItem.assert_called_once()
    call_args = girder.addMetadataToItem.call_args[0]
    assert call_args[0] == "item_env"
    assert "write_skipped" not in result
    metadata = ctx.get_output_metadata("result")
    assert metadata["tracking_item_source"].value == "env"


def test_manifest_skips_when_neither_set(monkeypatch):
    """No config value, no env var → skip Girder write."""
    monkeypatch.delenv("COORD_ENRICHMENT_MANIFEST_ITEM", raising=False)
    config = CoordEnrichmentConfig(dry_run=False)
    girder = _make_girder_mock()
    ctx = build_asset_context()

    result = coord_enrichment_manifest(ctx, config, _make_report(), girder)

    girder.addMetadataToItem.assert_not_called()
    assert result["write_skipped"] == "no_tracking_item_configured"
    metadata = ctx.get_output_metadata("result")
    assert metadata["tracking_item_source"].value == "unset"


def test_manifest_dry_run_with_env_var(monkeypatch):
    """dry_run=True skips the write even when env var is set."""
    monkeypatch.setenv("COORD_ENRICHMENT_MANIFEST_ITEM", "item_env")
    config = CoordEnrichmentConfig(dry_run=True)
    girder = _make_girder_mock()
    ctx = build_asset_context()

    result = coord_enrichment_manifest(ctx, config, _make_report(), girder)

    girder.addMetadataToItem.assert_not_called()
    assert result["write_skipped"] == "dry_run"
    metadata = ctx.get_output_metadata("result")
    assert metadata["tracking_item_source"].value == "env"
