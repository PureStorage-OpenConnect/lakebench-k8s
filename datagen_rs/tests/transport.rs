//! S3_PATH_STYLE, S3_VERIFY_SSL and S3_CA_CERT reach the S3 client (DAT-3);
//! read back from the builder, no network.

use datagen_rs::s3sink::{S3Cfg, S3Sink, Transport};
use object_store::aws::AmazonS3ConfigKey;
use object_store::ClientConfigKey;

fn ca() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/test-ca.pem")
        .to_string_lossy()
        .into_owned()
}

fn cfg(t: Transport) -> S3Cfg {
    S3Cfg {
        bucket: "b".into(),
        prefix: String::new(),
        endpoint: "http://127.0.0.1:9000".into(),
        region: "us-east-1".into(),
        access_key: "k".into(),
        secret_key: "s".into(),
        transport: t,
    }
}

fn get(b: &object_store::aws::AmazonS3Builder, k: AmazonS3ConfigKey) -> Option<String> {
    b.get_config_value(&k)
}

#[test]
fn defaults_are_path_style_verified_http_for_http() {
    let t = Transport::from_values("http://10.0.1.50:80", None, None, None).unwrap();
    assert!(!t.virtual_hosted && !t.allow_invalid_certificates && t.allow_http);
    let b = S3Sink::builder(&cfg(t)).unwrap();
    assert_eq!(
        get(&b, AmazonS3ConfigKey::VirtualHostedStyleRequest).as_deref(),
        Some("false")
    );
    let https = Transport::from_values("https://s3.example", Some("true"), None, None).unwrap();
    assert!(!https.allow_http);
}

#[test]
fn path_style_false_builds_virtual_hosted() {
    let t = Transport::from_values("http://h", Some("false"), Some("false"), None).unwrap();
    let b = S3Sink::builder(&cfg(t)).unwrap();
    assert_eq!(
        get(&b, AmazonS3ConfigKey::VirtualHostedStyleRequest).as_deref(),
        Some("true")
    );
    assert_eq!(
        get(
            &b,
            AmazonS3ConfigKey::Client(ClientConfigKey::AllowInvalidCertificates)
        )
        .as_deref(),
        Some("true")
    );
    assert_eq!(
        get(&b, AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp)).as_deref(),
        Some("true")
    );
}

#[test]
fn ca_cert_loaded() {
    let t = Transport::from_values("https://h", None, None, Some(&ca())).unwrap();
    assert_eq!(t.certificates().unwrap().len(), 1);
    assert!(S3Sink::builder(&cfg(t)).is_ok());
}

#[test]
fn bad_transport_values_are_refused() {
    assert!(Transport::from_values("http://h", Some("maybe"), None, None).is_err());
    assert!(Transport::from_values("http://h", None, Some("off"), None).is_err());
    let e =
        Transport::from_values("http://h", None, None, Some("/nonexistent/ca.pem")).unwrap_err();
    assert!(e.contains("S3_CA_CERT"));
    let not_pem = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    assert!(
        Transport::from_values("http://h", None, None, Some(not_pem.to_str().unwrap())).is_err()
    );
}
