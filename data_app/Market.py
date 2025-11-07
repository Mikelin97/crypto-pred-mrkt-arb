from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional, Union

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


class Market:
    """Helper for inspecting and fetching Polymarket contract snapshots."""

    bucket: str = "poly-punting-raw"

    def __init__(
        self,
        name: str,
        *,
        data_root: Optional[Union[str, Path]] = None,
    ) -> None:
        self.name = name
        self._asset = name.lower()
        self._data_root = (
            Path(data_root) if data_root else Path(__file__).resolve().parent / "data"
        )
        self._contract_dir = self._data_root / "contracts" / self._asset
        self._contract_dir.mkdir(parents=True, exist_ok=True)

        self.available_slugs = self._pull_available_slugs()
        self._slug_lookup = (
            dict(zip(self.available_slugs["slug"], self.available_slugs["key"]))
            if not self.available_slugs.empty
            else {}
        )

    def _pull_available_slugs(self) -> pd.DataFrame:
        prefix = f"contracts/{self._asset}/"
        rows: List[dict] = []

        try:
            paginator = s3.get_paginator("list_objects_v2")
        except Exception as exc:  # pragma: no cover - boto3 client misconfiguration
            logger.error("Failed to create S3 paginator: %s", exc)
            return pd.DataFrame(columns=["slug", "key", "size_mb", "last_modified"])

        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    slug = key.split("/")[-1]
                    rows.append(
                        {
                            "slug": slug,
                            "key": key,
                            "size_mb": round(item.get("Size", 0) / (1024 * 1024), 2),
                            "last_modified": item.get("LastModified"),
                        }
                    )
        except Exception as exc:
            logger.error("Error listing contents of bucket %s: %s", self.bucket, exc)

        if not rows:
            return pd.DataFrame(columns=["slug", "key", "size_mb", "last_modified"])

        df = pd.DataFrame(rows)
        df.sort_values(by="last_modified", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def refresh(self) -> None:
        """Refresh the available slug listing from S3."""
        self.available_slugs = self._pull_available_slugs()
        self._slug_lookup = (
            dict(zip(self.available_slugs["slug"], self.available_slugs["key"]))
            if not self.available_slugs.empty
            else {}
        )

    def _normalise_slug(self, slug: str) -> str:
        clean_slug = slug.strip()
        if not clean_slug.endswith(".jsonl"):
            clean_slug = f"{clean_slug}.jsonl"
        return clean_slug

    def _pull_slug(self, slug: str, *, download: bool, refresh: bool) -> Path:
        normalised_slug = self._normalise_slug(slug)
        key = self._slug_lookup.get(normalised_slug)

        if key is None:
            if refresh:
                self.refresh()
                key = self._slug_lookup.get(normalised_slug)
            if key is None:
                raise KeyError(
                    f"Slug '{normalised_slug}' not found for market '{self.name}'."
                )

        destination = self._contract_dir / normalised_slug
        if download and (refresh or not destination.exists()):
            try:
                s3.download_file(self.bucket, key, str(destination))
            except Exception as exc:  # pragma: no cover - relies on external service
                logger.error(
                    "Failed to download %s from %s: %s",
                    normalised_slug,
                    self.bucket,
                    exc,
                )
                raise
        return destination

    def get_available_slugs(self) -> pd.DataFrame:
        """Return a copy of the cached slug metadata."""
        return self.available_slugs.copy()

    def get_slug(
        self,
        slug: Union[str, Iterable[str]],
        *,
        download: bool = True,
        refresh: bool = False,
    ) -> Union[Path, List[Path]]:
        """
        Return local file path(s) for the provided slug or slugs.

        When `download` is True (default), missing files are downloaded from S3.
        Set `refresh` to True to refresh the slug inventory before resolving paths.
        """

        if isinstance(slug, str):
            return self._pull_slug(slug, download=download, refresh=refresh)

        paths: List[Path] = []
        for item in slug:
            paths.append(self._pull_slug(item, download=download, refresh=refresh))
        return paths
