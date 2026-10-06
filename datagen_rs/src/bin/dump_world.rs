//! Dump per-entity world attributes to Parquet for parity checking against the
//! Python golden (golden_world.parquet).

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use datagen_rs::model::build_world;

fn main() {
    let scale: f64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.01);
    let seed: i64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(42);
    let w = build_world(scale, seed, 60);
    let n = w.population;
    let idx = 1..=n;

    // Attributes are recomputed on demand to bound pod memory: the world no longer holds
    // the per-entity columns, so the dump recomputes each one via its method.
    let id: Vec<i64> = idx.clone().map(|i| i as i64).collect();
    let ty: Vec<i32> = idx.clone().map(|i| w.ty(i) as i32).collect();
    let country: Vec<&str> = idx.clone().map(|i| w.country(i)).collect();
    let name: Vec<String> = idx.clone().map(|i| w.name(i)).collect();
    let street: Vec<String> = idx.clone().map(|i| w.street(i)).collect();
    let town: Vec<String> = idx.clone().map(|i| w.town(i)).collect();
    let region: Vec<String> = idx.clone().map(|i| w.region(i)).collect();
    let postcode: Vec<String> = idx.clone().map(|i| w.postcode(i)).collect();
    let email: Vec<String> = idx.clone().map(|i| w.email(i)).collect();
    let iban: Vec<String> = idx.clone().map(|i| w.iban(i)).collect();
    let lei: Vec<String> = idx.clone().map(|i| w.lei(i)).collect();
    let bic: Vec<String> = idx.clone().map(|i| w.bic(i)).collect();
    let ccy: Vec<&str> = idx.clone().map(|i| w.ccy(i)).collect();
    let nacct: Vec<i32> = idx.clone().map(|i| w.n_accounts(i)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("type", DataType::Int32, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("town", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("iban", DataType::Utf8, false),
        Field::new("lei", DataType::Utf8, false),
        Field::new("bic", DataType::Utf8, false),
        Field::new("ccy", DataType::Utf8, false),
        Field::new("nacct", DataType::Int32, false),
    ]));

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(id)),
        Arc::new(Int32Array::from(ty)),
        Arc::new(StringArray::from_iter_values(country)),
        Arc::new(StringArray::from_iter_values(name)),
        Arc::new(StringArray::from_iter_values(street)),
        Arc::new(StringArray::from_iter_values(town)),
        Arc::new(StringArray::from_iter_values(region)),
        Arc::new(StringArray::from_iter_values(postcode)),
        Arc::new(StringArray::from_iter_values(email)),
        Arc::new(StringArray::from_iter_values(iban)),
        Arc::new(StringArray::from_iter_values(lei)),
        Arc::new(StringArray::from_iter_values(bic)),
        Arc::new(StringArray::from_iter_values(ccy)),
        Arc::new(Int32Array::from(nacct)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let file = File::create("/tmp/rs_world.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    eprintln!("wrote /tmp/rs_world.parquet {} rows", n);
}
