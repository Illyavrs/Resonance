//! Read-only media from a retained local .osz package. No archive member is
//! written to disk and the immutable package, chart hashes and IDs stay intact.
use crate::common::{ApiError, AppState, stored_file};
use axum::http::StatusCode;
use encoding_rs::SHIFT_JIS;
use illya_domain::beatmap_filename::valid_beatmap_filename;
use std::{
    collections::HashSet,
    io::{Cursor, Read},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::Semaphore;

const ARCHIVE_LIMIT: usize = 64 * 1024 * 1024;
const ENTRY_LIMIT: usize = 512;
const TOTAL_LIMIT: u64 = 256 * 1024 * 1024;
const CHART_LIMIT: usize = 8 * 1024 * 1024;
const COVER_LIMIT: usize = 2 * 1024 * 1024;
const AUDIO_LIMIT: usize = 16 * 1024 * 1024;
static WORKERS: OnceLock<Arc<Semaphore>> = OnceLock::new();

pub(super) async fn load(
    state: &AppState,
    set: i64,
    preview: bool,
) -> Result<(Vec<u8>, &'static str), ApiError> {
    crate::native_maps::require_id(set)?;
    if set < crate::native_maps::LOCAL_ID_MIN {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_local_map_set"));
    }
    crate::native_maps::require_active(&state.pool, set).await?;
    let permit = WORKERS
        .get_or_init(|| Arc::new(Semaphore::new(2)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "local_media_busy"))?;
    let path = crate::native_maps::archive_path(state, set)
        .await?
        .ok_or_else(missing)?;
    let bytes = stored_file(state, &path, "maps", ARCHIVE_LIMIT as u64).await?;
    let job = tokio::task::spawn_blocking(move || {
        // Dropping the HTTP future cannot release a busy decompression slot.
        let _permit = permit;
        extract(&bytes, preview)
    });
    let media = tokio::time::timeout(Duration::from_secs(8), job)
        .await
        .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "local_media_timeout"))?
        .map_err(|_| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "local_media_failed"))??;
    // Withdrawal also takes effect during storage reads or blocking ZIP work.
    crate::native_maps::require_active(&state.pool, set).await?;
    Ok(media)
}

fn invalid() -> ApiError {
    ApiError(
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_local_media_archive",
    )
}

fn missing() -> ApiError {
    ApiError(StatusCode::NOT_FOUND, "local_media_not_found")
}

fn slice(bytes: &[u8], start: usize, length: usize) -> Result<&[u8], ApiError> {
    bytes
        .get(start..start.checked_add(length).ok_or_else(invalid)?)
        .ok_or_else(invalid)
}

fn word(bytes: &[u8], start: usize) -> Result<u16, ApiError> {
    Ok(u16::from_le_bytes(
        slice(bytes, start, 2)?.try_into().unwrap(),
    ))
}

fn signature(bytes: &[u8], start: usize) -> Result<u32, ApiError> {
    Ok(u32::from_le_bytes(
        slice(bytes, start, 4)?.try_into().unwrap(),
    ))
}

fn wide(bytes: &[u8], start: usize) -> Result<u64, ApiError> {
    Ok(u64::from_le_bytes(
        slice(bytes, start, 8)?.try_into().unwrap(),
    ))
}

/// Check the record count before the ZIP library allocates its filename index.
/// A small compressed package can otherwise advertise thousands of tiny files.
fn advertised_count(bytes: &[u8]) -> Result<usize, ApiError> {
    if bytes.len() < 22 {
        return Err(invalid());
    }
    let start = bytes.len().saturating_sub(22 + usize::from(u16::MAX));
    let mut footer = None;
    for at in start..=bytes.len() - 22 {
        if signature(bytes, at)? == 0x0605_4b50
            && at + 22 + usize::from(word(bytes, at + 20)?) == bytes.len()
            && footer.replace(at).is_some()
        {
            return Err(invalid());
        }
    }
    let footer = footer.ok_or_else(invalid)?;
    if word(bytes, footer + 4)? != 0 || word(bytes, footer + 6)? != 0 {
        return Err(invalid());
    }
    let disk_count = word(bytes, footer + 8)?;
    let narrow_count = word(bytes, footer + 10)?;
    let count = if footer >= 20 && signature(bytes, footer - 20)? == 0x0706_4b50 {
        let locator = footer - 20;
        if signature(bytes, locator + 4)? != 0 || signature(bytes, locator + 16)? != 1 {
            return Err(invalid());
        }
        // Locate the wide footer by its declared ending, including packages
        // with a prepended stub. Every candidate read stays inside the archive.
        let mut record = None;
        for (at, marker) in bytes[..locator].windows(4).enumerate() {
            if marker != 0x0606_4b50_u32.to_le_bytes() {
                continue;
            }
            let length = wide(bytes, at + 4)?;
            if length >= 44
                && (at as u64)
                    .checked_add(12)
                    .and_then(|offset| offset.checked_add(length))
                    == Some(locator as u64)
                && record.replace(at).is_some()
            {
                return Err(invalid());
            }
        }
        let at = record.ok_or_else(invalid)?;
        if signature(bytes, at + 16)? != 0 || signature(bytes, at + 20)? != 0 {
            return Err(invalid());
        }
        let count = wide(bytes, at + 32)?;
        if wide(bytes, at + 24)? != count
            || (narrow_count != u16::MAX && u64::from(narrow_count) != count)
            || (disk_count != u16::MAX && u64::from(disk_count) != count)
        {
            return Err(invalid());
        }
        count
    } else {
        if disk_count != narrow_count || narrow_count == u16::MAX {
            return Err(invalid());
        }
        u64::from(narrow_count)
    };
    if count == 0 || count > ENTRY_LIMIT as u64 {
        return Err(invalid());
    }
    Ok(count as usize)
}

/// Decode Windows archive names before handling separators. A backslash byte
/// can be part of a Japanese multibyte character, rather than a separator.
fn cp932(raw: &[u8]) -> Result<String, ApiError> {
    let mut decoded = String::new();
    let mut chunk = 0;
    let mut at = 0;
    while at < raw.len() {
        match raw[at] {
            0x81..=0x9f | 0xe0..=0xfc => {
                if at + 1 == raw.len() {
                    return Err(invalid());
                }
                at += 2;
                continue;
            }
            0xa0 | 0xfd..=0xff => {
                decoded.push_str(
                    &SHIFT_JIS
                        .decode_without_bom_handling_and_without_replacement(&raw[chunk..at])
                        .ok_or_else(invalid)?,
                );
                let code = match raw[at] {
                    0xa0 => 0xf8f0,
                    other => 0xf8f1 + u32::from(other - 0xfd),
                };
                decoded.push(char::from_u32(code).ok_or_else(invalid)?);
                chunk = at + 1;
            }
            _ => {}
        }
        at += 1;
    }
    decoded.push_str(
        &SHIFT_JIS
            .decode_without_bom_handling_and_without_replacement(&raw[chunk..])
            .ok_or_else(invalid)?,
    );
    Ok(decoded)
}

fn decoded_name(raw: &[u8], flags: u16, extra: &[u8]) -> Result<String, ApiError> {
    let mut override_name = None;
    let mut at = 0;
    while at < extra.len() {
        let kind = word(extra, at)?;
        let length = usize::from(word(extra, at + 2)?);
        let content = slice(extra, at + 4, length)?;
        if kind == 0x7075 {
            if override_name.is_some()
                || content.len() < 5
                || content[0] != 1
                || signature(content, 1)? != crc32fast::hash(raw)
            {
                return Err(invalid());
            }
            override_name = Some(
                std::str::from_utf8(&content[5..])
                    .map_err(|_| invalid())?
                    .to_owned(),
            );
        }
        at += 4 + length;
    }
    if let Some(name) = override_name {
        Ok(name)
    } else if flags & (1 << 11) != 0 {
        Ok(std::str::from_utf8(raw).map_err(|_| invalid())?.to_owned())
    } else {
        cp932(raw)
    }
}

fn normalized(name: &str) -> Result<String, ApiError> {
    let name = name.replace('\\', "/");
    if name.starts_with('/')
        || name.chars().any(|c| c.is_control())
        || name
            .split('/')
            .any(|part| part == ".." || part.contains(':'))
    {
        return Err(invalid());
    }
    let name = name
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/")
        .to_lowercase();
    if name.is_empty() {
        return Err(invalid());
    }
    Ok(name)
}

struct Member {
    key: String,
    index: usize,
    size: u64,
    directory: bool,
}

/// Inspect original directory records as well as zip's index: identical ZIP
/// names can be collapsed by the library, so its indexed count alone is weak.
fn members(
    bytes: &[u8],
    zip: &mut zip::ZipArchive<Cursor<&[u8]>>,
) -> Result<Vec<Member>, ApiError> {
    if zip.is_empty() || zip.len() > ENTRY_LIMIT {
        return Err(invalid());
    }
    let mut at = usize::try_from(zip.central_directory_start()).map_err(|_| invalid())?;
    let mut list = Vec::new();
    let mut keys = HashSet::new();
    let mut total = 0_u64;
    while signature(bytes, at)? == 0x0201_4b50 {
        if list.len() == ENTRY_LIMIT || list.len() == zip.len() {
            return Err(invalid());
        }
        let header = slice(bytes, at, 46)?;
        let flags = word(header, 8)?;
        if flags & (1 | (1 << 6) | (1 << 13)) != 0 {
            return Err(invalid());
        }
        let name_length = usize::from(word(header, 28)?);
        let extra_length = usize::from(word(header, 30)?);
        let comment_length = usize::from(word(header, 32)?);
        let raw_name = slice(bytes, at + 46, name_length)?;
        let extra = slice(bytes, at + 46 + name_length, extra_length)?;
        let name = decoded_name(raw_name, flags, extra)?;
        let key = normalized(&name)?;
        if !keys.insert(key.clone()) {
            return Err(invalid());
        }
        let index = list.len();
        let entry = zip.by_index_raw(index).map_err(|_| invalid())?;
        if entry.central_header_start() != at as u64
            || entry.encrypted()
            || entry.is_symlink()
            || !matches!(
                entry.compression(),
                zip::CompressionMethod::Stored | zip::CompressionMethod::Deflated
            )
            || entry.size() > ARCHIVE_LIMIT as u64
        {
            return Err(invalid());
        }
        total = total.checked_add(entry.size()).ok_or_else(invalid)?;
        if total > TOTAL_LIMIT {
            return Err(invalid());
        }
        let local = usize::try_from(entry.header_start()).map_err(|_| invalid())?;
        let local_header = slice(bytes, local, 30)?;
        let local_flags = word(local_header, 6)?;
        if signature(local_header, 0)? != 0x0403_4b50
            || local_flags & (1 | (1 << 6) | (1 << 13)) != 0
            || (local_flags ^ flags) & (1 << 11) != 0
            || word(local_header, 8)? != word(header, 10)?
            || slice(bytes, local + 30, usize::from(word(local_header, 26)?))? != raw_name
        {
            return Err(invalid());
        }
        let directory = name.ends_with('/') || name.ends_with('\\');
        list.push(Member {
            key,
            index,
            size: entry.size(),
            directory,
        });
        at = at
            .checked_add(46 + name_length + extra_length + comment_length)
            .ok_or_else(invalid)?;
    }
    if list.len() != zip.len()
        || !matches!(
            signature(bytes, at)?,
            0x0605_4b50 | 0x0606_4b50 | 0x0505_4b50
        )
    {
        return Err(invalid());
    }
    Ok(list)
}

fn read_member(
    zip: &mut zip::ZipArchive<Cursor<&[u8]>>,
    member: &Member,
    limit: usize,
) -> Result<Vec<u8>, ApiError> {
    if member.directory || member.size > limit as u64 {
        return Err(invalid());
    }
    let mut entry = zip.by_index(member.index).map_err(|_| invalid())?;
    let mut bytes = Vec::with_capacity((member.size as usize).min(limit));
    entry
        .by_ref()
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() > limit || bytes.len() as u64 != member.size {
        return Err(invalid());
    }
    Ok(bytes)
}

/// Events filenames can contain commas and doubled quotes. Interpret the CSV
/// structure instead of selecting arbitrary text between the first two quotes.
fn event_fields(line: &str) -> Result<Vec<String>, ApiError> {
    let mut chars = line.chars().peekable();
    let mut fields = Vec::new();
    loop {
        while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            chars.next();
        }
        let mut value = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next() {
                    Some('"') if chars.peek() == Some(&'"') => {
                        chars.next();
                        value.push('"');
                    }
                    Some('"') => break,
                    Some(c) => value.push(c),
                    None => return Err(invalid()),
                }
            }
            while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
                chars.next();
            }
            if chars.peek().is_some_and(|c| *c != ',') {
                return Err(invalid());
            }
        } else {
            while chars.peek().is_some_and(|c| *c != ',') {
                let c = chars.next().unwrap();
                if c == '"' {
                    return Err(invalid());
                }
                value.push(c);
            }
            value = value.trim().to_owned();
        }
        fields.push(value);
        if fields.len() > 16 {
            return Err(invalid());
        }
        if chars.next().is_none() {
            return Ok(fields);
        }
    }
}

fn media_reference(chart: &[u8], preview: bool) -> Result<String, ApiError> {
    let text = std::str::from_utf8(chart).map_err(|_| invalid())?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut lines = text.lines();
    if !lines.next().is_some_and(|line| {
        line.trim()
            .strip_prefix("osu file format v")
            .is_some_and(|version| version.parse::<u16>().is_ok())
    }) {
        return Err(invalid());
    }
    let mut section = "";
    let mut audio = None;
    let mut background = None;
    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line;
            continue;
        }
        if section == "[General]"
            && let Some((key, value)) = line.split_once(':')
            && key.trim().eq_ignore_ascii_case("AudioFilename")
            && audio.replace(value.trim().to_owned()).is_some()
        {
            return Err(invalid());
        }
        if section == "[Events]" {
            let kind = line.split(',').next().unwrap_or_default().trim();
            if kind == "0" || kind == "Background" {
                let fields = event_fields(line)?;
                if fields.len() < 3
                    || fields[1].parse::<i64>().is_err()
                    || background.replace(fields[2].clone()).is_some()
                {
                    return Err(invalid());
                }
            }
        }
    }
    let reference = if preview { audio } else { background }.ok_or_else(missing)?;
    normalized(&reference)
}

fn media_type(bytes: &[u8], preview: bool) -> Result<&'static str, ApiError> {
    let mime = if preview {
        if bytes.starts_with(b"OggS") {
            Some("audio/ogg")
        } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
            Some("audio/wav")
        } else if bytes.starts_with(b"ID3")
            || (bytes.len() >= 4
                && bytes[0] == 0xff
                && bytes[1] & 0xe0 == 0xe0
                && bytes[1] & 0x18 != 0x08
                && bytes[1] & 0x06 != 0
                && !matches!(bytes[2] & 0xf0, 0 | 0xf0)
                && bytes[2] & 0x0c != 0x0c)
        {
            Some("audio/mpeg")
        } else {
            None
        }
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    };
    mime.ok_or(ApiError(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_local_media_format",
    ))
}

fn extract(bytes: &[u8], preview: bool) -> Result<(Vec<u8>, &'static str), ApiError> {
    if bytes.len() > ARCHIVE_LIMIT {
        return Err(invalid());
    }
    let count = advertised_count(bytes)?;
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).map_err(|_| invalid())?;
    if zip.len() != count {
        return Err(invalid());
    }
    let members = members(bytes, &mut zip)?;
    let chart = members
        .iter()
        .filter(|member| {
            !member.directory
                && valid_beatmap_filename(member.key.rsplit('/').next().unwrap_or_default())
        })
        .min_by(|left, right| left.key.cmp(&right.key))
        .ok_or_else(missing)?;
    let chart_bytes = read_member(&mut zip, chart, CHART_LIMIT)?;
    let reference = media_reference(&chart_bytes, preview)?;
    let relative = if let Some((directory, _)) = chart.key.rsplit_once('/') {
        format!("{directory}/{reference}")
    } else {
        reference
    };
    let member = members
        .iter()
        .find(|member| member.key == relative)
        .ok_or_else(missing)?;
    let media = read_member(
        &mut zip,
        member,
        if preview { AUDIO_LIMIT } else { COVER_LIMIT },
    )?;
    let mime = media_type(&media, preview)?;
    // Serve retained audio unchanged (up to 16 MiB). This is a playback source,
    // not a newly generated or transcoded preview clip.
    Ok((media, mime))
}

#[cfg(test)]
#[path = "native_local_media_tests.rs"]
mod tests;
