//! Original, in-memory ZIP fixtures for the native local-media boundary.
//! The contract checks MIME signatures; these fixtures do not claim that a
//! signature-only payload is a decodable image or a playable audio stream.
use super::*;
use std::io::{Cursor, Write};
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const PNG: &[u8] = b"\x89PNG\r\n\x1a\nfixture-cover";
const JPEG: &[u8] = b"\xff\xd8\xfffixture-cover";
const WEBP: &[u8] = b"RIFF\x10\x00\x00\x00WEBPfixture-cover";
const MP3: &[u8] = b"ID3fixture-audio";
const OGG: &[u8] = b"OggSfixture-audio";
const WAV: &[u8] = b"RIFF\x10\x00\x00\x00WAVEfixture-audio";

fn chart(audio: &str, background: &str) -> Vec<u8> {
    format!(
        "osu file format v14\n[General]\nAudioFilename: {audio}\n[Metadata]\nTitle: Original media fixture\n[Events]\n0,0,\"{background}\",0,0\n"
    )
    .into_bytes()
}

fn package(members: &[(&str, &[u8])], method: CompressionMethod) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(method);
    for (name, bytes) in members {
        writer.start_file(*name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn valid_package(method: CompressionMethod) -> Vec<u8> {
    let metadata = chart("audio.mp3", "cover.png");
    package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
        ],
        method,
    )
}

fn padded_package(padding_count: usize, padding: &[u8]) -> Vec<u8> {
    let metadata = chart("audio.mp3", "cover.png");
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    for (name, bytes) in [
        ("fixture.osu", metadata.as_slice()),
        ("audio.mp3", MP3),
        ("cover.png", PNG),
    ] {
        writer.start_file(name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    for index in 0..padding_count {
        writer
            .start_file(format!("padding-{index}.bin"), options)
            .unwrap();
        writer.write_all(padding).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[test]
fn local_media_extracts_referenced_cover_and_audio_from_stored_and_deflated_zips() {
    for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
        let bytes = valid_package(method);
        assert_eq!(extract(&bytes, false).unwrap(), (PNG.to_vec(), "image/png"));
        assert_eq!(extract(&bytes, true).unwrap(), (MP3.to_vec(), "audio/mpeg"));
    }
}

#[test]
fn local_media_accepts_all_supported_cover_signatures() {
    for (name, bytes, mime) in [
        ("cover.png", PNG, "image/png"),
        ("cover.jpg", JPEG, "image/jpeg"),
        ("cover.webp", WEBP, "image/webp"),
    ] {
        let metadata = chart("audio.mp3", name);
        let archive = package(
            &[
                ("fixture.osu", &metadata),
                ("audio.mp3", MP3),
                (name, bytes),
            ],
            CompressionMethod::Stored,
        );
        assert_eq!(extract(&archive, false).unwrap(), (bytes.to_vec(), mime));
    }
}

#[test]
fn local_media_accepts_all_supported_audio_signatures() {
    for (name, bytes, mime) in [
        ("audio.mp3", MP3, "audio/mpeg"),
        ("audio.ogg", OGG, "audio/ogg"),
        ("audio.wav", WAV, "audio/wav"),
    ] {
        let metadata = chart(name, "cover.png");
        let archive = package(
            &[
                ("fixture.osu", &metadata),
                (name, bytes),
                ("cover.png", PNG),
            ],
            CompressionMethod::Stored,
        );
        assert_eq!(extract(&archive, true).unwrap(), (bytes.to_vec(), mime));
    }
}

#[test]
fn local_media_resolves_assets_relative_to_the_selected_chart_directory() {
    let metadata = chart("assets/audio.mp3", "assets/cover.png");
    let archive = package(
        &[
            ("charts/fixture.osu", &metadata),
            ("charts/assets/audio.mp3", MP3),
            ("charts/assets/cover.png", PNG),
            ("assets/audio.mp3", OGG),
            ("assets/cover.png", JPEG),
        ],
        CompressionMethod::Stored,
    );
    assert_eq!(
        extract(&archive, false).unwrap(),
        (PNG.to_vec(), "image/png")
    );
    assert_eq!(
        extract(&archive, true).unwrap(),
        (MP3.to_vec(), "audio/mpeg")
    );
}

#[test]
fn local_media_selects_the_first_chart_by_normalized_filename_not_zip_order() {
    let first = chart("first.mp3", "first.png");
    let later = chart("later.ogg", "later.jpg");
    let archive = package(
        &[
            ("z-later.osu", &later),
            ("a-first.osu", &first),
            ("later.ogg", OGG),
            ("later.jpg", JPEG),
            ("first.mp3", MP3),
            ("first.png", PNG),
        ],
        CompressionMethod::Deflated,
    );
    assert_eq!(
        extract(&archive, false).unwrap(),
        (PNG.to_vec(), "image/png")
    );
    assert_eq!(
        extract(&archive, true).unwrap(),
        (MP3.to_vec(), "audio/mpeg")
    );
}

#[test]
fn local_media_matches_references_case_insensitively() {
    let metadata = chart("AUDIO.MP3", "COVER.PNG");
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
        ],
        CompressionMethod::Stored,
    );
    assert_eq!(
        extract(&archive, false).unwrap(),
        (PNG.to_vec(), "image/png")
    );
    assert_eq!(
        extract(&archive, true).unwrap(),
        (MP3.to_vec(), "audio/mpeg")
    );
}

#[test]
fn local_media_parses_quoted_background_commas_and_numeric_or_named_events() {
    for event in ["0", "Background"] {
        let metadata = format!(
            "osu file format v14\r\n[General]\r\nAudioFilename: audio.mp3\r\n[Events]\r\n// Original comment\r\n{event},0,\"cover, summer.png\",0,0\r\n"
        );
        let archive = package(
            &[
                ("fixture.osu", metadata.as_bytes()),
                ("audio.mp3", MP3),
                ("cover, summer.png", PNG),
            ],
            CompressionMethod::Stored,
        );
        assert_eq!(
            extract(&archive, false).unwrap(),
            (PNG.to_vec(), "image/png")
        );
    }
}

#[test]
fn local_media_does_not_choose_unreferenced_assets_when_metadata_is_absent() {
    for metadata in [
        b"osu file format v14\n[Metadata]\nTitle: No media references\n".as_slice(),
        b"osu file format v14\n[General]\nAudioFilename: missing.mp3\n[Events]\n0,0,\"missing.png\",0,0\n".as_slice(),
    ] {
        let archive = package(
            &[
                ("fixture.osu", metadata),
                ("audio.mp3", MP3),
                ("cover.png", PNG),
            ],
            CompressionMethod::Stored,
        );
        assert!(extract(&archive, false).is_err());
        assert!(extract(&archive, true).is_err());
    }
}

#[test]
fn local_media_does_not_use_video_events_as_backgrounds() {
    let metadata = b"osu file format v14\n[General]\nAudioFilename: audio.mp3\n[Events]\nVideo,0,\"cover.png\",0,0\n";
    let archive = package(
        &[
            ("fixture.osu", metadata.as_slice()),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
        ],
        CompressionMethod::Stored,
    );
    assert!(extract(&archive, false).is_err());
    assert_eq!(
        extract(&archive, true).unwrap(),
        (MP3.to_vec(), "audio/mpeg")
    );
}

#[test]
fn local_media_rejects_invalid_utf8_chart_metadata() {
    let mut metadata = chart("audio.mp3", "cover.png");
    metadata.push(0xff);
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
        ],
        CompressionMethod::Stored,
    );
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_repeated_audio_filename_fields_even_if_values_match() {
    for duplicate in ["AudioFilename: audio.mp3", "audiofilename: other.mp3"] {
        let metadata = format!(
            "osu file format v14\n[General]\nAudioFilename: audio.mp3\n{duplicate}\n[Events]\n0,0,\"cover.png\",0,0\n"
        );
        let archive = package(
            &[
                ("fixture.osu", metadata.as_bytes()),
                ("audio.mp3", MP3),
                ("other.mp3", MP3),
                ("cover.png", PNG),
            ],
            CompressionMethod::Stored,
        );
        assert!(extract(&archive, true).is_err());
    }
}

#[test]
fn local_media_rejects_unsafe_metadata_paths() {
    for path in [
        "../audio.mp3",
        "/audio.mp3",
        "C:/audio.mp3",
        "C:\\audio.mp3",
        "https://example.invalid/audio.mp3",
        "assets/../../audio.mp3",
    ] {
        let metadata = chart(path, path);
        let archive = package(
            &[
                ("fixture.osu", &metadata),
                ("audio.mp3", MP3),
                ("cover.png", PNG),
            ],
            CompressionMethod::Stored,
        );
        assert!(extract(&archive, false).is_err(), "cover path: {path}");
        assert!(extract(&archive, true).is_err(), "audio path: {path}");
    }
}

#[test]
fn local_media_rejects_duplicate_casefolded_member_paths() {
    let metadata = chart("audio.mp3", "cover.png");
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
            ("COVER.PNG", PNG),
        ],
        CompressionMethod::Stored,
    );
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_traversal_members_even_when_they_are_not_referenced() {
    let metadata = chart("audio.mp3", "cover.png");
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
            ("../outside.txt", b"unreferenced"),
        ],
        CompressionMethod::Stored,
    );
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_encrypted_member_flags() {
    let mut archive = valid_package(CompressionMethod::Stored);
    let local = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x03\x04")
        .unwrap();
    let central = archive
        .windows(4)
        .position(|bytes| bytes == b"PK\x01\x02")
        .unwrap();
    // Mark the first member encrypted in both headers. No encrypted contents
    // are produced: the reader must reject before exposing any selected media.
    archive[local + 6] |= 1;
    archive[central + 8] |= 1;
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_symlink_members() {
    let metadata = chart("audio.mp3", "cover.png");
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default();
    for (name, bytes) in [
        ("fixture.osu", metadata.as_slice()),
        ("audio.mp3", MP3),
        ("cover.png", PNG),
    ] {
        writer.start_file(name, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer
        .add_symlink("linked.png", "cover.png", options)
        .unwrap();
    let archive = writer.finish().unwrap().into_inner();
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_invalid_and_truncated_media_signatures() {
    for invalid in [
        b"html".as_slice(),
        b"\x89PNG".as_slice(),
        b"RIFF".as_slice(),
    ] {
        let metadata = chart("audio.mp3", "cover.png");
        let archive = package(
            &[
                ("fixture.osu", &metadata),
                ("audio.mp3", invalid),
                ("cover.png", invalid),
            ],
            CompressionMethod::Stored,
        );
        assert!(extract(&archive, false).is_err());
        assert!(extract(&archive, true).is_err());
    }
}

#[test]
fn local_media_rejects_archives_without_a_chart_and_non_zip_inputs() {
    let no_chart = package(
        &[("audio.mp3", MP3), ("cover.png", PNG)],
        CompressionMethod::Stored,
    );
    for archive in [no_chart.as_slice(), b"not an archive".as_slice()] {
        assert!(extract(archive, false).is_err());
        assert!(extract(archive, true).is_err());
    }
}

#[test]
fn local_media_rejects_more_than_512_members() {
    let boundary = padded_package(509, b"x");
    assert_eq!(
        extract(&boundary, false).unwrap(),
        (PNG.to_vec(), "image/png")
    );
    assert_eq!(
        extract(&boundary, true).unwrap(),
        (MP3.to_vec(), "audio/mpeg")
    );
    let archive = padded_package(510, b"x");
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_compressed_archives_over_64_mib() {
    // Each extra member stays below the member limit. Stored contents plus
    // ZIP headers push the otherwise valid media package over the input cap.
    let archive = padded_package(8, &vec![0; 8 * 1024 * 1024]);
    assert!(archive.len() > 64 * 1024 * 1024);
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_selected_chart_larger_than_8_mib() {
    let mut metadata = chart("audio.mp3", "cover.png");
    metadata.resize(8 * 1024 * 1024 + 1, b' ');
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", PNG),
        ],
        CompressionMethod::Deflated,
    );
    assert!(extract(&archive, false).is_err());
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_rejects_selected_cover_larger_than_2_mib() {
    let metadata = chart("audio.mp3", "cover.png");
    let mut cover = PNG.to_vec();
    cover.resize(2 * 1024 * 1024 + 1, 0);
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", MP3),
            ("cover.png", &cover),
        ],
        CompressionMethod::Deflated,
    );
    assert!(extract(&archive, false).is_err());
}

#[test]
fn local_media_rejects_selected_audio_larger_than_16_mib() {
    let metadata = chart("audio.mp3", "cover.png");
    let mut audio = MP3.to_vec();
    audio.resize(16 * 1024 * 1024 + 1, 0);
    let archive = package(
        &[
            ("fixture.osu", &metadata),
            ("audio.mp3", &audio),
            ("cover.png", PNG),
        ],
        CompressionMethod::Deflated,
    );
    assert!(extract(&archive, true).is_err());
}

#[test]
fn local_media_decodes_cp932_backslash_trail_bytes_before_path_normalization() {
    // Windows Shift-JIS uses 0x5c as the second byte of ソ. It must not become
    // a path separator before the two-byte character has been decoded.
    let raw = [0x83, 0x5c, b'.', b'p', b'n', b'g'];
    assert_eq!(cp932(&raw).unwrap(), "ソ.png");
    assert_eq!(decoded_name(&raw, 0, &[]).unwrap(), "ソ.png");
    assert_eq!(
        normalized(&decoded_name(&raw, 0, &[]).unwrap()).unwrap(),
        "ソ.png"
    );
}

#[test]
fn local_media_decodes_windows_cp932_private_single_bytes() {
    assert_eq!(
        cp932(&[0xa0, 0xfd, 0xfe, 0xff]).unwrap(),
        "\u{f8f0}\u{f8f1}\u{f8f2}\u{f8f3}"
    );
}

#[test]
fn local_media_rejects_incomplete_and_invalid_cp932_lead_pairs() {
    for raw in [
        [0x81].as_slice(),
        [0x83].as_slice(),
        [0xfc].as_slice(),
        [0x81, 0x7f].as_slice(),
    ] {
        assert!(cp932(raw).is_err(), "invalid bytes: {raw:?}");
        assert!(decoded_name(raw, 0, &[]).is_err());
    }
}

fn unicode_path_extra(raw: &[u8], name: &str) -> Vec<u8> {
    let mut payload = vec![1];
    payload.extend_from_slice(&crc32fast::hash(raw).to_le_bytes());
    payload.extend_from_slice(name.as_bytes());
    let mut extra = 0x7075_u16.to_le_bytes().to_vec();
    extra.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_le_bytes());
    extra.extend_from_slice(&payload);
    extra
}

#[test]
fn local_media_uses_a_validated_unicode_path_override() {
    let raw = [0x83, 0x5c, b'.', b'p', b'n', b'g'];
    let extra = unicode_path_extra(&raw, "夏の空.png");
    assert_eq!(decoded_name(&raw, 0, &extra).unwrap(), "夏の空.png");
}

#[test]
fn local_media_rejects_unicode_path_overrides_with_wrong_crc_or_duplicate_records() {
    let raw = b"cover.png";
    let extra = unicode_path_extra(raw, "夏の空.png");
    let mut wrong_crc = extra.clone();
    wrong_crc[5] ^= 1;
    assert!(decoded_name(raw, 0, &wrong_crc).is_err());
    let mut duplicate = extra.clone();
    duplicate.extend_from_slice(&extra);
    assert!(decoded_name(raw, 0, &duplicate).is_err());
}

#[test]
fn local_media_respects_utf8_zip_flags_and_rejects_invalid_utf8_names() {
    assert_eq!(
        decoded_name("夏の空.png".as_bytes(), 1 << 11, &[]).unwrap(),
        "夏の空.png"
    );
    assert!(decoded_name(&[0x83, 0x5c], 1 << 11, &[]).is_err());
}

#[test]
fn local_media_event_csv_preserves_doubled_quotes_and_quoted_commas() {
    assert_eq!(
        event_fields("0,0,\"cover, \"\"blue\"\".png\",0,0").unwrap(),
        ["0", "0", "cover, \"blue\".png", "0", "0"]
    );
}

#[test]
fn local_media_event_csv_rejects_unterminated_quotes_and_text_after_a_quote() {
    for event in [
        "0,0,\"cover.png",
        "0,0,\"cover.png\"suffix,0,0",
        "0,0,cover\".png,0,0",
    ] {
        assert!(event_fields(event).is_err(), "malformed event: {event}");
    }
}

fn footer_offset(bytes: &[u8]) -> usize {
    let at = bytes.len() - 22;
    assert_eq!(&bytes[at..at + 4], b"PK\x05\x06");
    assert_eq!(&bytes[at + 20..], &[0, 0]);
    at
}

#[test]
fn local_media_rejects_an_oversized_advertised_count_before_zip_indexing() {
    let mut archive = valid_package(CompressionMethod::Stored);
    assert_eq!(advertised_count(&archive).unwrap(), 3);
    let footer = footer_offset(&archive);
    archive[footer + 8..footer + 10].copy_from_slice(&513_u16.to_le_bytes());
    archive[footer + 10..footer + 12].copy_from_slice(&513_u16.to_le_bytes());
    assert!(advertised_count(&archive).is_err());
}

fn zip64_count_footer(count: u64, narrow_count: u16) -> Vec<u8> {
    // This is a footer-parser fixture only. The record is deliberately not a
    // full ZIP package, and these tests do not claim extraction compatibility.
    let mut record = vec![0; 56];
    record[..4].copy_from_slice(&0x0606_4b50_u32.to_le_bytes());
    record[4..12].copy_from_slice(&44_u64.to_le_bytes());
    record[12..14].copy_from_slice(&45_u16.to_le_bytes());
    record[14..16].copy_from_slice(&45_u16.to_le_bytes());
    record[24..32].copy_from_slice(&count.to_le_bytes());
    record[32..40].copy_from_slice(&count.to_le_bytes());
    let mut locator = vec![0; 20];
    locator[..4].copy_from_slice(&0x0706_4b50_u32.to_le_bytes());
    locator[16..20].copy_from_slice(&1_u32.to_le_bytes());
    let mut eocd = vec![0; 22];
    eocd[..4].copy_from_slice(&0x0605_4b50_u32.to_le_bytes());
    eocd[8..10].copy_from_slice(&narrow_count.to_le_bytes());
    eocd[10..12].copy_from_slice(&narrow_count.to_le_bytes());
    record.extend_from_slice(&locator);
    record.extend_from_slice(&eocd);
    record
}

#[test]
fn local_media_accepts_zip64_advertised_counts_with_matching_or_sentinel_narrow_fields() {
    for narrow in [3, u16::MAX] {
        let footer = zip64_count_footer(3, narrow);
        assert_eq!(advertised_count(&footer).unwrap(), 3);
    }
}

#[test]
fn local_media_rejects_oversized_or_disagreeing_zip64_advertised_counts() {
    for footer in [
        zip64_count_footer(513, u16::MAX),
        zip64_count_footer(3, 4),
        zip64_count_footer(0, 0),
    ] {
        assert!(advertised_count(&footer).is_err());
    }
    let mut mismatched_disk = zip64_count_footer(3, 3);
    mismatched_disk[24..32].copy_from_slice(&2_u64.to_le_bytes());
    assert!(advertised_count(&mismatched_disk).is_err());
}
