"""Deterministic metadata-only ZIP packages and bounded validation."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
import zlib
from collections import Counter
from typing import Any

from media_catalog.content_repository import canonical_json
from media_catalog.errors import DomainError

MAX_UPLOAD_BYTES = 8 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 24 * 1024 * 1024
MAX_MEMBERS = 8


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _export_item(item: dict[str, Any]) -> dict[str, Any]:
    fields = ("id", "kind", "name", "synopsis", "genre", "releaseYear", "language", "externalId", "status", "version", "createdAt", "updatedAt")
    result = {field: item[field] for field in fields if field in item}
    if item.get("kind") == "movie":
        result.update({field: item[field] for field in ("runtimeMinutes", "streamUrl") if field in item})
    else:
        result["seasons"] = [
            {
                "id": season["id"],
                "seasonNumber": season["seasonNumber"],
                "episodes": [
                    {field: episode[field] for field in ("id", "name", "episodeNumber", "synopsis", "runtimeMinutes", "streamUrl") if field in episode}
                    for episode in season.get("episodes", [])
                ],
            }
            for season in item.get("seasons", [])
        ]
    result["licenses"] = [
        {key: license[key] for key in ("id", "region", "startsAt", "endsAt", "rightsHolder") if key in license}
        for license in item.get("licenses", [])
    ]
    result["assets"] = [
        {key: asset[key] for key in ("id", "path", "role", "mimeType", "sizeBytes", "checksumSha256") if key in asset}
        for asset in item.get("assets", [])
    ]
    return result


def build_export(items: list[dict[str, Any]]) -> tuple[bytes, dict[str, Any]]:
    ordered = sorted((_export_item(item) for item in items), key=lambda item: (item.get("kind", ""), item.get("id", 0)))
    content_bytes = canonical_json({"items": ordered}).encode("utf-8")
    manifest = {
        "format": "media-catalog-metadata",
        "schemaVersion": 1,
        "itemCount": len(ordered),
        "contentSha256": _sha256(content_bytes),
        "entries": {"content.json": _sha256(content_bytes)},
    }
    manifest_bytes = canonical_json(manifest).encode("utf-8")
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, strict_timestamps=True) as archive:
        for filename, data in (("content.json", content_bytes), ("manifest.json", manifest_bytes)):
            info = zipfile.ZipInfo(filename, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            info.create_system = 3
            archive.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    package = output.getvalue()
    manifest["packageSha256"] = _sha256(package)
    return package, manifest


def validate_export(data: bytes) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    if len(data) > MAX_UPLOAD_BYTES:
        raise DomainError(413, "package_too_large", "ZIP package exceeds the 8 MiB upload limit")
    try:
        archive = zipfile.ZipFile(io.BytesIO(data), "r")
    except (zipfile.BadZipFile, OSError):
        return {"valid": False, "findings": [{"code": "invalid_zip", "message": "The upload is not a valid ZIP package"}]}
    with archive:
        infos = archive.infolist()
        if len(infos) > MAX_MEMBERS:
            findings.append({"code": "too_many_members", "message": f"ZIP packages may contain at most {MAX_MEMBERS} members"})
        names = [info.filename for info in infos]
        duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
        for name in duplicates:
            findings.append({"code": "duplicate_entry", "entry": name, "message": "ZIP member names must be unique"})
        total_size = sum(info.file_size for info in infos)
        if total_size > MAX_UNCOMPRESSED_BYTES:
            findings.append({"code": "expanded_package_too_large", "message": "Expanded ZIP content exceeds the 24 MiB limit"})
        safe_names = {"manifest.json", "content.json"}
        for info in infos:
            name = info.filename
            parts = name.replace("\\", "/").split("/")
            if name.startswith(("/", "\\")) or "\\" in name or any(part in {"", ".", ".."} for part in parts) or ":" in parts[0]:
                findings.append({"code": "unsafe_entry_path", "entry": name, "message": "ZIP members must use safe relative paths"})
            if name not in safe_names:
                findings.append({"code": "unsupported_entry", "entry": name, "message": "Only manifest.json and content.json are supported"})
            if info.file_size > MAX_UNCOMPRESSED_BYTES:
                findings.append({"code": "entry_too_large", "entry": name, "message": "ZIP member exceeds the expanded size limit"})
            if info.flag_bits & 0x1:
                findings.append({"code": "encrypted_entry", "entry": name, "message": "Encrypted ZIP members are not supported"})
            if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                findings.append({"code": "unsupported_compression", "entry": name, "message": "ZIP member compression method is not supported"})
        missing = safe_names - set(names)
        for name in sorted(missing):
            findings.append({"code": "missing_entry", "entry": name, "message": f"Package is missing {name}"})
        if findings:
            return {"valid": False, "findings": findings}
        try:
            manifest = json.loads(archive.read("manifest.json"))
            content_bytes = archive.read("content.json")
            content = json.loads(content_bytes)
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError, OSError, RuntimeError, NotImplementedError, EOFError, zipfile.BadZipFile, zlib.error):
            return {"valid": False, "findings": [{"code": "invalid_json", "message": "Package entries must contain valid JSON"}]}
        if not isinstance(manifest, dict):
            findings.append({"code": "invalid_manifest", "message": "manifest.json must contain an object"})
            manifest = {}
        if manifest.get("format") != "media-catalog-metadata" or manifest.get("schemaVersion") != 1:
            findings.append({"code": "unsupported_manifest", "message": "Manifest format or schemaVersion is unsupported"})
        actual_checksum = _sha256(content_bytes)
        checksums = manifest.get("entries")
        expected_entry_checksum = checksums.get("content.json") if isinstance(checksums, dict) else None
        if manifest.get("contentSha256") != actual_checksum or expected_entry_checksum != actual_checksum:
            findings.append({"code": "checksum_mismatch", "entry": "content.json", "message": "Content checksum does not match the manifest"})
        if not isinstance(content, dict) or not isinstance(content.get("items"), list):
            findings.append({"code": "invalid_content", "message": "content.json must contain an items array"})
        elif manifest.get("itemCount") != len(content["items"]):
            findings.append({"code": "item_count_mismatch", "message": "Manifest itemCount does not match content.json"})
        return {"valid": not findings, "findings": findings, "manifest": {key: value for key, value in manifest.items() if key != "packageSha256"}, "itemCount": len(content["items"]) if isinstance(content, dict) and isinstance(content.get("items"), list) else None, "packageSha256": _sha256(data)}
