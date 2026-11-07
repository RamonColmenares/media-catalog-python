from __future__ import annotations

import io
import struct
import zipfile

from media_catalog.exports import build_export, validate_export


def _package() -> bytes:
    package, _ = build_export([{"id": 1, "kind": "movie", "name": "Synthetic Package", "synopsis": "Metadata only.", "assets": []}])
    return package


def test_zip_validator_reports_unsafe_paths_missing_members_and_large_expansion() -> None:
    unsafe_stream = io.BytesIO()
    with zipfile.ZipFile(unsafe_stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("content.json", '{"items": []}')
        archive.writestr("../outside.json", "not extracted")
    unsafe = validate_export(unsafe_stream.getvalue())
    assert unsafe["valid"] is False
    assert {finding["code"] for finding in unsafe["findings"]} >= {"unsafe_entry_path", "unsupported_entry"}

    missing_stream = io.BytesIO()
    with zipfile.ZipFile(missing_stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", "{}")
    missing = validate_export(missing_stream.getvalue())
    assert "missing_entry" in {finding["code"] for finding in missing["findings"]}

    expanded_stream = io.BytesIO()
    with zipfile.ZipFile(expanded_stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        archive.writestr("content.json", b"0" * (25 * 1024 * 1024))
    expanded = validate_export(expanded_stream.getvalue())
    assert expanded["valid"] is False
    assert "expanded_package_too_large" in {finding["code"] for finding in expanded["findings"]}


def test_zip_validator_returns_findings_for_encryption_and_unknown_compression() -> None:
    encrypted = bytearray(_package())
    local = encrypted.index(b"PK\x03\x04")
    central = encrypted.index(b"PK\x01\x02")
    struct.pack_into("<H", encrypted, local + 6, struct.unpack_from("<H", encrypted, local + 6)[0] | 1)
    struct.pack_into("<H", encrypted, central + 8, struct.unpack_from("<H", encrypted, central + 8)[0] | 1)
    result = validate_export(bytes(encrypted))
    assert result["valid"] is False
    assert "encrypted_entry" in {finding["code"] for finding in result["findings"]}

    unsupported = bytearray(_package())
    local = unsupported.index(b"PK\x03\x04")
    central = unsupported.index(b"PK\x01\x02")
    struct.pack_into("<H", unsupported, local + 8, 99)
    struct.pack_into("<H", unsupported, central + 10, 99)
    result = validate_export(bytes(unsupported))
    assert result["valid"] is False
    assert "unsupported_compression" in {finding["code"] for finding in result["findings"]}
