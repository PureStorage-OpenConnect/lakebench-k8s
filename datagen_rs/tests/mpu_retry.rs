//! A failed multipart completion is retried by rebuilding the file from the
//! same batch on the same key, and the object written is byte-identical to a
//! first-time success (DAT-3 "MPU complete retry").

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datagen_rs::s3sink::{
    with_finish_retries, write_parquet_retrying, MpuWriter, FINISH_RETRY_WAITS_S,
};
use datagen_rs::writer::writer_properties;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{MultipartUpload, ObjectStore, PutPayload, PutResult, UploadPart};
use parquet::arrow::ArrowWriter;

#[derive(Debug)]
struct FailingComplete {
    inner: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for FailingComplete {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let _ = self.inner.abort().await;
        Err(object_store::Error::Generic {
            store: "test",
            source: "injected CompleteMultipartUpload failure".into(),
        })
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let n = 20_000i64;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..n).map(|i| format!("row-{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn encode(b: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, b.schema(), Some(writer_properties())).unwrap();
    w.write(b).unwrap();
    w.close().unwrap();
    buf
}

#[test]
fn mpu_finish_retry_rewrites_same_bytes() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let store = Arc::new(InMemory::new());
    let key = Path::from("bronze/part-000000.parquet");
    let b = batch();
    let attempts = AtomicUsize::new(0);
    let mut waits = Vec::new();
    // The production writer (generate.rs's continuous path calls the same
    // function), with an opener whose first upload fails its completion.
    let size = write_parquet_retrying(
        "test",
        || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            let upload = rt.block_on(store.put_multipart(&key)).unwrap();
            let upload: Box<dyn MultipartUpload> = if n == 0 {
                Box::new(FailingComplete { inner: upload })
            } else {
                upload
            };
            MpuWriter::from_upload(upload, rt.handle().clone(), key.to_string())
        },
        &b,
        writer_properties,
        |s| waits.push(s),
    )
    .expect("the second attempt succeeds");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(waits, vec![FINISH_RETRY_WAITS_S[0]]);
    let got = rt
        .block_on(async { store.get(&key).await.unwrap().bytes().await.unwrap() })
        .to_vec();
    let want = encode(&b);
    assert_eq!(got.len() as u64, size);
    assert_eq!(
        got, want,
        "the retried object differs from a first-time write"
    );
}

#[test]
fn finish_gives_up_after_the_last_wait() {
    let calls = AtomicUsize::new(0);
    let mut waits = Vec::new();
    let r: Result<(), String> = with_finish_retries(
        "test",
        || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("boom".to_string())
        },
        |s| waits.push(s),
    );
    assert!(r.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), FINISH_RETRY_WAITS_S.len() + 1);
    assert_eq!(waits, FINISH_RETRY_WAITS_S.to_vec());
}
