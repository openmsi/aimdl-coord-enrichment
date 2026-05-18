"""Tests for the parent-inheritance helper (Phase 4, Step 1)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from aimdl_coord_enrichment.coord_enrichment.inheritance import (
    InheritedCoords,
    inherit_from_parent,
    inherited_station_coord_source,
)
from aimdl_coord_enrichment.instruments.types import ResolutionError


def _parent(
    item_id="parent_1",
    data_type="pdv_trace",
    station_x=11.0,
    station_y=-5.0,
    source_timestamp="2026-02-18T18:45:56+00:00",
    transform_version="HELIX/v1",
    include_prov=True,
):
    meta = {
        "Station_X": station_x,
        "Station_Y": station_y,
        "data_type": data_type,
    }
    if include_prov:
        meta["coord_provenance"] = {
            "instrument": "HELIX",
            "transform_version": transform_version,
            "source_timestamp": source_timestamp,
        }
    return {"_id": item_id, "meta": meta}


def _derived(derived_id="derived_1", parent_id="parent_1"):
    return {
        "_id": derived_id,
        "meta": {"prov": {"wasDerivedFrom": parent_id}},
    }


class TestInheritFromParentHappyPath:
    def test_inherit_happy_path(self):
        parent = _parent()
        result = inherit_from_parent(
            _derived(), girder=None, fetch_item=lambda _: parent
        )
        assert result.station_x == 11.0
        assert result.station_y == -5.0
        assert result.parent_item_id == "parent_1"
        assert result.parent_data_type == "pdv_trace"
        assert result.parent_transform_version == "HELIX/v1"
        assert result.parent_source_timestamp == datetime(
            2026, 2, 18, 18, 45, 56, tzinfo=timezone.utc
        )
        assert result.parent_source_timestamp.tzinfo is not None


class TestInheritFromParentErrors:
    def test_inherit_missing_prov_raises(self):
        item = {"_id": "d1", "meta": {}}
        with pytest.raises(ResolutionError, match="no prov.wasDerivedFrom"):
            inherit_from_parent(item, girder=None)

    def test_inherit_parent_fetch_fails_raises(self):
        def boom(item_id):
            raise ConnectionError("timeout")

        with pytest.raises(ResolutionError, match="parent fetch failed.*parent_1"):
            inherit_from_parent(_derived(), girder=None, fetch_item=boom)

    def test_inherit_parent_returns_unusable_value_raises(self):
        with pytest.raises(ResolutionError, match="unusable value"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: None
            )

    def test_inherit_parent_returns_empty_dict_raises(self):
        with pytest.raises(ResolutionError, match="unusable value"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: {}
            )

    def test_inherit_parent_missing_station_raises(self):
        parent = _parent(station_x=None)
        with pytest.raises(ResolutionError, match="parent not yet enriched"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )

    def test_inherit_parent_missing_coord_provenance_raises(self):
        parent = _parent(include_prov=False)
        with pytest.raises(ResolutionError, match="missing coord_provenance"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )

    def test_inherit_parent_timestamp_naive_raises(self):
        parent = _parent(source_timestamp="2026-02-18T18:45:56")
        with pytest.raises(ResolutionError, match="naive"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )

    def test_inherit_parent_timestamp_unparseable_raises(self):
        parent = _parent(source_timestamp="not-a-date")
        with pytest.raises(ResolutionError, match="unparseable"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )

    def test_inherit_parent_missing_transform_version_raises(self):
        parent = _parent(transform_version="")
        with pytest.raises(ResolutionError, match="transform_version absent"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )

    def test_inherit_parent_missing_data_type_raises(self):
        parent = _parent(data_type="")
        with pytest.raises(ResolutionError, match="missing meta.data_type"):
            inherit_from_parent(
                _derived(), girder=None, fetch_item=lambda _: parent
            )


class TestInheritZuluTimestamp:
    def test_inherit_parent_timestamp_with_zulu_suffix_parsed(self):
        parent = _parent(source_timestamp="2026-04-16T16:56:16Z")
        result = inherit_from_parent(
            _derived(), girder=None, fetch_item=lambda _: parent
        )
        assert result.parent_source_timestamp == datetime(
            2026, 4, 16, 16, 56, 16, tzinfo=timezone.utc
        )


class TestInheritedStationCoordSource:
    def test_inherited_station_coord_source_shape(self):
        coords = InheritedCoords(
            station_x=1.0,
            station_y=2.0,
            parent_source_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            parent_transform_version="HELIX/v1",
            parent_item_id="p1",
            parent_data_type="pdv_trace",
        )
        result = inherited_station_coord_source(coords)
        assert result == {
            "kind": "inherited",
            "parent_item_id": "p1",
            "parent_data_type": "pdv_trace",
        }
        assert set(result.keys()) == {"kind", "parent_item_id", "parent_data_type"}


class TestGirderGetFallback:
    def test_inherit_calls_girder_get_when_fetch_item_not_provided(self):
        parent = _parent()
        mock_girder = MagicMock()
        mock_girder.get.return_value = parent
        result = inherit_from_parent(_derived(), girder=mock_girder)
        mock_girder.get.assert_called_once_with("item/parent_1")
        assert result.station_x == 11.0
