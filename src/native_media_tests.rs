use super::*;
use axum::{body::to_bytes, http::Request, routing::get};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn generated_resource_urls_stay_on_exact_private_origin() {
    let origin = public_origin("https://community.example:8443/").unwrap();
    let mut set = json!({"id":2630282,"covers":{"card":"https://attacker.invalid/secret"},"preview_url":"//b.ppy.sh/preview/1.mp3"});
    decorate_set_at(&mut set, 2630282, &origin);
    assert_eq!(
        set["preview_url"],
        "https://community.example:8443/api/v2/beatmapsets/2630282/preview"
    );
    assert_eq!(set["covers"].as_object().unwrap().len(), 8);
    for variant in COVER_VARIANTS {
        assert_eq!(
            set["covers"][variant],
            format!("https://community.example:8443/api/v2/beatmapsets/2630282/media/{variant}")
        );
    }
    assert!(!set.to_string().contains("attacker.invalid"));
    decorate_set_at(&mut set, native_maps::LOCAL_ID_MIN, &origin);
    assert!(
        set["preview_url"]
            .as_str()
            .unwrap()
            .contains("/2000000000/preview")
    );
    for invalid in [
        "https://osu.ppy.sh",
        "https://ppy.sh",
        "https://user:secret@community.example",
        "https://community.example/path",
        "https://community.example/?url=https://evil.example",
        "https://community.example/#x",
        "file:///etc/private",
        "http://community.example",
        "http://10.0.0.1",
    ] {
        assert!(public_origin(invalid).is_err(), "{invalid}");
    }
    for valid in [
        "https://illya.test",
        "https://community.example:8443",
        "http://localhost:3000",
        "http://127.0.0.1:8080",
        "http://[::1]:8080",
        "http://illya.test",
    ] {
        assert!(public_origin(valid).is_ok(), "{valid}");
    }
}

#[test]
fn upstream_paths_are_fixed_numeric_resources_and_local_ids_never_leave_server() {
    for (index, variant) in COVER_VARIANTS.into_iter().enumerate() {
        let url = upstream_url(MediaKey {
            set: 2630282,
            kind: Kind::Cover(index),
        })
        .unwrap();
        assert_eq!(
            url,
            format!("https://assets.ppy.sh/beatmaps/2630282/covers/{variant}.jpg")
        );
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert!(
            parsed.query().is_none() && parsed.fragment().is_none() && parsed.username().is_empty()
        );
    }
    assert_eq!(
        upstream_url(MediaKey {
            set: 42,
            kind: Kind::Preview
        })
        .unwrap(),
        "https://b.ppy.sh/preview/42.mp3"
    );
    for set in [-1, 0, native_maps::LOCAL_ID_MIN, i32::MAX as i64, i64::MAX] {
        assert!(
            upstream_url(MediaKey {
                set,
                kind: Kind::Preview
            })
            .is_err()
        );
    }
    assert!(
        upstream_url(MediaKey {
            set: 42,
            kind: Kind::Cover(8)
        })
        .is_err()
    );
}

#[test]
fn resources_require_bounded_matching_binary_type() {
    assert_eq!(
        validate_body(
            b"ID3\x04\0preview",
            Kind::Preview,
            "audio/mpeg; charset=binary"
        )
        .unwrap(),
        "audio/mpeg"
    );
    assert_eq!(
        validate_body(b"OggS\0preview", Kind::Preview, "audio/ogg").unwrap(),
        "audio/ogg"
    );
    assert_eq!(
        validate_body(b"RIFF\0\0\0\0WAVEpreview", Kind::Preview, "audio/wav").unwrap(),
        "audio/wav"
    );
    for (bytes, kind, mime) in [
        (&b"<html>login</html>"[..], Kind::Cover(0), "image/jpeg"),
        (&b"ID3audio"[..], Kind::Cover(0), "audio/mpeg"),
        (&b"ID3audio"[..], Kind::Preview, "text/html"),
        (&b""[..], Kind::Preview, "audio/mpeg"),
        (&b"RIFF1234AVI "[..], Kind::Preview, "audio/wav"),
    ] {
        assert!(validate_body(bytes, kind, mime).is_err());
    }
    assert!(validate_body(&vec![0; COVER_LIMIT + 1], Kind::Cover(0), "image/jpeg").is_err());
    assert!(validate_body(&vec![0; PREVIEW_LIMIT + 1], Kind::Preview, "audio/mpeg").is_err());
}

#[test]
fn cover_headers_require_real_supported_images_and_bounded_dimensions() {
    for (format, mime) in [
        (image::ImageFormat::Jpeg, "image/jpeg"),
        (image::ImageFormat::Png, "image/png"),
        (image::ImageFormat::WebP, "image/webp"),
    ] {
        let mut output = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            24,
            12,
            image::Rgb([20, 30, 40]),
        ))
        .write_to(&mut output, format)
        .unwrap();
        assert_eq!(
            validate_body(output.get_ref(), Kind::Cover(0), mime).unwrap(),
            mime
        );
        assert!(validate_body(&output.get_ref()[..8], Kind::Cover(0), mime).is_err());
    }
    let mut output = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(image::RgbImage::new(4097, 1))
        .write_to(&mut output, image::ImageFormat::Png)
        .unwrap();
    assert!(validate_body(output.get_ref(), Kind::Cover(0), "image/png").is_err());
}

#[test]
fn single_byte_ranges_handle_prefix_suffix_and_reject_ambiguous_requests() {
    for (value, expected) in [
        ("bytes=0-0", (0, 0)),
        ("bytes=2-5", (2, 5)),
        ("bytes=2-", (2, 9)),
        ("bytes=-4", (6, 9)),
        ("bytes=-40", (0, 9)),
        ("bytes=0-99", (0, 9)),
    ] {
        assert_eq!(byte_range(value, 10).unwrap(), expected);
    }
    for value in [
        "bytes=-0",
        "bytes=10-",
        "bytes=5-2",
        "bytes=0-1,4-5",
        "bytes=+0-1",
        "bytes= 0-1",
        "Bytes=0-1",
        "bytes=-",
        "items=0-1",
    ] {
        assert!(byte_range(value, 10).is_err(), "{value}");
    }
    assert!(byte_range("bytes=0-", 0).is_err());
}

#[tokio::test]
async fn responses_support_native_audio_ranges_and_revalidation_without_redirects() {
    let resource = media(b"ID3-preview".to_vec(), Kind::Preview, "audio/mpeg").unwrap();
    let mut request = HeaderMap::new();
    request.insert(header::RANGE, HeaderValue::from_static("bytes=4-6"));
    let partial = response(resource.clone(), &request);
    assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(partial.headers()[header::CONTENT_RANGE], "bytes 4-6/11");
    assert_eq!(partial.headers()[header::CONTENT_TYPE], "audio/mpeg");
    assert_eq!(
        partial.headers()[header::CACHE_CONTROL],
        "private, no-cache"
    );
    assert_eq!(partial.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
    assert!(!partial.headers().contains_key(header::LOCATION));
    assert_eq!(to_bytes(partial.into_body(), 100).await.unwrap(), "pre");
    request.insert(header::IF_NONE_MATCH, resource.etag.clone());
    let conditional = response(resource.clone(), &request);
    assert_eq!(conditional.status(), StatusCode::NOT_MODIFIED);
    assert!(
        to_bytes(conditional.into_body(), 100)
            .await
            .unwrap()
            .is_empty()
    );
    request.remove(header::IF_NONE_MATCH);
    request.insert(header::RANGE, HeaderValue::from_static("bytes=999-"));
    let invalid = response(resource.clone(), &request);
    assert_eq!(invalid.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(invalid.headers()[header::CONTENT_RANGE], "bytes */11");
    request.insert(header::RANGE, HeaderValue::from_static("bytes=0-1"));
    request.insert(
        header::IF_RANGE,
        HeaderValue::from_static("\"old-content\""),
    );
    let fresh = response(resource, &request);
    assert_eq!(fresh.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(fresh.into_body(), 100).await.unwrap(),
        "ID3-preview"
    );
}

#[tokio::test]
async fn fixed_fetch_does_not_forward_auth_and_rejects_redirects_wrong_types_and_oversize() {
    let calls = Arc::new(AtomicUsize::new(0));
    let capture = calls.clone();
    let app = Router::new()
        .route(
            "/preview",
            get(move |request: Request<Body>| {
                let calls = capture.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert!(!request.headers().contains_key(header::AUTHORIZATION));
                    assert!(!request.headers().contains_key(header::COOKIE));
                    assert!(!request.headers().contains_key("x-illya-service-key"));
                    ([(header::CONTENT_TYPE, "audio/mpeg")], "ID3-preview")
                }
            }),
        )
        .route(
            "/redirect",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [(header::LOCATION, "http://127.0.0.1:1/private")],
                )
            }),
        )
        .route(
            "/wrong",
            get(|| async { ([(header::CONTENT_TYPE, "text/html")], "ID3-preview") }),
        )
        .route(
            "/huge",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "image/jpeg")],
                    vec![0_u8; COVER_LIMIT + 1],
                )
            }),
        )
        .route(
            "/huge-stream",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "image/jpeg")
                    .body(Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::io::Error>(vec![0_u8; COVER_LIMIT]),
                        Ok(vec![0_u8]),
                    ])))
                    .unwrap()
            }),
        )
        .route("/missing", get(|| async { StatusCode::NOT_FOUND }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let client = http().unwrap();
    let result = fetch(&client, &format!("http://{address}/preview"), Kind::Preview)
        .await
        .unwrap();
    assert_eq!(result.bytes, "ID3-preview");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for (path, kind, status) in [
        ("redirect", Kind::Preview, StatusCode::BAD_GATEWAY),
        ("wrong", Kind::Preview, StatusCode::BAD_GATEWAY),
        ("huge", Kind::Cover(0), StatusCode::BAD_GATEWAY),
        ("huge-stream", Kind::Cover(0), StatusCode::BAD_GATEWAY),
        ("missing", Kind::Preview, StatusCode::NOT_FOUND),
    ] {
        let error = fetch(&client, &format!("http://{address}/{path}"), kind)
            .await
            .err()
            .unwrap();
        assert_eq!(error.0, status);
    }
    stop.send(()).unwrap();
    server.await.unwrap();
}
