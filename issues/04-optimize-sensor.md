# Issue: Optimize sensor to avoid recursive folder crawl

## Problem

The `helix_folder_sensor` calls `list_all_spreadsheet_items()` which recursively
walks the entire HELIX folder tree every hour, listing items in every subfolder
to find new CSV/XLSX files. As the folder tree grows, this becomes increasingly slow.

## Proposed Change

Replace the recursive crawl with a targeted query:
- Fetch recent items from the HELIX folder sorted by creation date (newest first)
- Only check a limited number of recent items instead of the full tree
- Use the sensor cursor to track the last-seen timestamp, not just seen item IDs

This reduces the sensor evaluation from walking potentially hundreds of folders
to a single paginated query for recent items.

## Labels

`enhancement`, `backend`, `stage-4`
