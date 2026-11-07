"""Strict request models for the local media operations API."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class WireModel(BaseModel):
    """Shared JSON rules: reject misspellings instead of silently dropping them."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", str_strip_whitespace=True)


def _required_text(value: str, label: str, maximum: int = 300) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must not be blank")
    if len(normalized) > maximum:
        raise ValueError(f"{label} must be at most {maximum} characters")
    return normalized


def _http_url(value: str | None, label: str) -> str | None:
    if value is None or value == "":
        return None
    try:
        parts = urlsplit(value)
        hostname = parts.hostname
        parts.port
    except ValueError as error:
        raise ValueError(f"{label} must be a valid absolute HTTP or HTTPS URL") from error
    if parts.scheme not in {"http", "https"} or not hostname or parts.username or parts.password:
        raise ValueError(f"{label} must be an absolute HTTP or HTTPS URL")
    return value


class ContentFields(WireModel):
    name: str = Field(min_length=1, max_length=300)
    synopsis: str = Field(default="", max_length=5000)
    genre: str = Field(default="Uncategorized", min_length=1, max_length=100)
    release_year: int | None = Field(default=None, alias="releaseYear", ge=1888, le=2200)
    language: str = Field(default="und", min_length=2, max_length=35)
    external_id: str | None = Field(default=None, alias="externalId", max_length=180)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _required_text(value, "name")

    @field_validator("release_year")
    @classmethod
    def validate_release_year(cls, value: int | None) -> int | None:
        if value is not None and value > datetime.now(timezone.utc).year + 5:
            raise ValueError("releaseYear cannot be more than five years in the future")
        return value

    @field_validator("genre")
    @classmethod
    def validate_genre(cls, value: str) -> str:
        return _required_text(value, "genre", 100)

    @field_validator("language")
    @classmethod
    def validate_language(cls, value: str) -> str:
        return _required_text(value, "language", 35).lower()

    @field_validator("external_id")
    @classmethod
    def validate_external_id(cls, value: str | None) -> str | None:
        return _required_text(value, "externalId", 180) if value is not None else None


class MovieCreate(ContentFields):
    stream_url: str | None = Field(default=None, alias="streamUrl")
    runtime_minutes: int | None = Field(default=None, alias="runtimeMinutes", ge=1, le=1000)

    @field_validator("stream_url")
    @classmethod
    def validate_stream_url(cls, value: str | None) -> str | None:
        return _http_url(value, "streamUrl")


class MovieUpdate(MovieCreate):
    expected_version: int = Field(alias="expectedVersion", ge=1)


class EpisodeInput(WireModel):
    id: int | None = Field(default=None, gt=0)
    name: str = Field(min_length=1, max_length=300)
    episode_number: int = Field(alias="episodeNumber", gt=0, le=10000)
    synopsis: str = Field(default="", max_length=5000)
    runtime_minutes: int | None = Field(default=None, alias="runtimeMinutes", ge=1, le=1000)
    stream_url: str | None = Field(default=None, alias="streamUrl")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _required_text(value, "episode name")

    @field_validator("stream_url")
    @classmethod
    def validate_stream_url(cls, value: str | None) -> str | None:
        return _http_url(value, "streamUrl")


class SeasonInput(WireModel):
    id: int | None = Field(default=None, gt=0)
    season_number: int = Field(alias="seasonNumber", gt=0, le=1000)
    episodes: list[EpisodeInput] = Field(default_factory=list, max_length=500)

    @model_validator(mode="after")
    def unique_episode_numbers(self) -> "SeasonInput":
        numbers = [episode.episode_number for episode in self.episodes]
        if len(numbers) != len(set(numbers)):
            raise ValueError("episodeNumber values must be unique within a season")
        ids = [episode.id for episode in self.episodes if episode.id is not None]
        if len(ids) != len(set(ids)):
            raise ValueError("episode IDs must be unique within a season")
        return self


class SeriesCreate(ContentFields):
    seasons: list[SeasonInput] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def unique_season_numbers(self) -> "SeriesCreate":
        numbers = [season.season_number for season in self.seasons]
        if len(numbers) != len(set(numbers)):
            raise ValueError("seasonNumber values must be unique within a series")
        season_ids = [season.id for season in self.seasons if season.id is not None]
        if len(season_ids) != len(set(season_ids)):
            raise ValueError("season IDs must be unique within a series")
        episode_ids = [episode.id for season in self.seasons for episode in season.episodes if episode.id is not None]
        if len(episode_ids) != len(set(episode_ids)):
            raise ValueError("episode IDs must be unique within a series")
        return self


class SeriesUpdate(SeriesCreate):
    seasons: list[SeasonInput] = Field(max_length=100)
    expected_version: int = Field(alias="expectedVersion", ge=1)


class TransitionRequest(WireModel):
    action: Literal["publish", "unpublish", "archive", "restore"]
    expected_version: int = Field(alias="expectedVersion", ge=1)
    reason: str = Field(default="", max_length=500)


class LicenseInput(WireModel):
    region: str = Field(min_length=2, max_length=5)
    starts_at: datetime = Field(alias="startsAt")
    ends_at: datetime = Field(alias="endsAt")
    rights_holder: str = Field(alias="rightsHolder", min_length=1, max_length=200)

    @field_validator("region")
    @classmethod
    def normalize_region(cls, value: str) -> str:
        normalized = value.upper()
        if normalized != "WORLD" and not re.fullmatch(r"[A-Z]{2}", normalized):
            raise ValueError("region must be WORLD or a two-letter country code")
        return normalized

    @field_validator("starts_at", "ends_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("license timestamps must include a timezone")
        return value.astimezone(timezone.utc)

    @field_validator("rights_holder")
    @classmethod
    def validate_holder(cls, value: str) -> str:
        return _required_text(value, "rightsHolder", 200)

    @model_validator(mode="after")
    def valid_window(self) -> "LicenseInput":
        if self.ends_at <= self.starts_at:
            raise ValueError("endsAt must be later than startsAt")
        return self


class AssetInput(WireModel):
    path: str = Field(min_length=1, max_length=500)
    role: Literal["poster", "backdrop", "video", "audio", "subtitle", "transcript", "caption", "document", "other"]
    mime_type: str = Field(alias="mimeType", min_length=3, max_length=120)
    size_bytes: int = Field(alias="sizeBytes", ge=0, le=10_000_000_000)
    checksum_sha256: str = Field(alias="checksumSha256", pattern=r"^[a-fA-F0-9]{64}$")

    @field_validator("path")
    @classmethod
    def safe_relative_path(cls, value: str) -> str:
        path = value.replace("\\", "/")
        parts = path.split("/")
        if path.startswith("/") or "\x00" in path or ":" in path or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("path must be a safe relative asset path")
        return path

    @field_validator("mime_type")
    @classmethod
    def normalize_mime(cls, value: str) -> str:
        normalized = _required_text(value, "mimeType", 120).lower()
        if not re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", normalized):
            raise ValueError("mimeType must use the type/subtype form")
        return normalized

    @field_validator("checksum_sha256")
    @classmethod
    def normalize_checksum(cls, value: str) -> str:
        return value.lower()


class LabelInput(WireModel):
    name: str = Field(min_length=1, max_length=80)
    color: str = Field(default="#64748b", pattern=r"^#[0-9a-fA-F]{6}$")

    @field_validator("name")
    @classmethod
    def normalized_name(cls, value: str) -> str:
        return _required_text(value, "label name", 80).casefold()


class LabelUpdate(LabelInput):
    expected_version: int = Field(alias="expectedVersion", ge=1)


class CollectionInput(WireModel):
    name: str = Field(min_length=1, max_length=180)
    description: str = Field(default="", max_length=2000)
    items: list["ContentReference"] = Field(default_factory=list, max_length=1000)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _required_text(value, "collection name", 180)

    @model_validator(mode="after")
    def unique_members(self) -> "CollectionInput":
        refs = [(item.kind, item.id) for item in self.items]
        if len(refs) != len(set(refs)):
            raise ValueError("collection members must be unique")
        return self


class CollectionUpdate(WireModel):
    name: str = Field(min_length=1, max_length=180)
    description: str = Field(default="", max_length=2000)
    expected_version: int = Field(alias="expectedVersion", ge=1)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _required_text(value, "collection name", 180)


class ContentReference(WireModel):
    kind: Literal["movie", "series"]
    id: int = Field(gt=0)


class ReorderInput(WireModel):
    ids: list[int] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_ids(self) -> "ReorderInput":
        if len(self.ids) != len(set(self.ids)):
            raise ValueError("reorder IDs must be unique")
        return self


class CollectionReorderInput(WireModel):
    items: list[ContentReference] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_items(self) -> "CollectionReorderInput":
        refs = [(item.kind, item.id) for item in self.items]
        if len(refs) != len(set(refs)):
            raise ValueError("collection reorder references must be unique")
        return self


class SearchQuery(WireModel):
    text: str | None = Field(default=None, max_length=300)
    kind: Literal["movie", "series"] | None = None
    genre: str | None = Field(default=None, max_length=100)
    status: Literal["draft", "published", "archived"] | None = None
    language: str | None = Field(default=None, max_length=35)
    release_year_from: int | None = Field(default=None, alias="releaseYearFrom", ge=1888, le=2200)
    release_year_to: int | None = Field(default=None, alias="releaseYearTo", ge=1888, le=2200)
    label: str | None = Field(default=None, max_length=80)
    sort: Literal["name", "releaseYear", "updatedAt", "kind"] = "name"
    direction: Literal["asc", "desc"] = "asc"
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0, le=1_000_000)

    @model_validator(mode="after")
    def valid_year_range(self) -> "SearchQuery":
        if self.release_year_from and self.release_year_to and self.release_year_from > self.release_year_to:
            raise ValueError("releaseYearFrom cannot exceed releaseYearTo")
        return self


class SavedSearchInput(WireModel):
    name: str = Field(min_length=1, max_length=180)
    query: SearchQuery


class SavedSearchUpdate(SavedSearchInput):
    expected_version: int = Field(alias="expectedVersion", ge=1)


class ImportRow(WireModel):
    kind: Literal["movie", "series"]
    item: dict[str, Any]


class ImportPreview(WireModel):
    rows: list[ImportRow] = Field(min_length=1, max_length=500)


class ImportCommit(ImportPreview):
    idempotency_key: str = Field(alias="idempotencyKey", min_length=8, max_length=120)


class ExportRequest(WireModel):
    references: list[ContentReference] = Field(min_length=1, max_length=1000)


class ExportValidation(WireModel):
    manifest: dict[str, Any]
    entries: dict[str, str]
