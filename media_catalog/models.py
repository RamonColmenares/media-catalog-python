"""Wire models shared by the media services."""

from pydantic import BaseModel, ConfigDict, Field


class WireModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class Movie(WireModel):
    id: int | None = None
    name: str | None = None
    genre: str | None = None
    ulr_stream: str | None = Field(default=None, alias="ulrStream")


class Chapter(WireModel):
    id: int | None = None
    name: str | None = None
    number: int = 0
    url_stream: str | None = Field(default=None, alias="urlStream")


class Season(WireModel):
    id: int | None = None
    season_number: int = Field(default=0, alias="seasonNumber")
    chapters: list[Chapter] | None = None


class Series(WireModel):
    id: int | None = None
    name: str | None = None
    genre: str | None = None
    seasons: list[Season] | None = None
