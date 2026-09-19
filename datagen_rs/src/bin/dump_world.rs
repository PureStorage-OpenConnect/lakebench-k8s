//! Dump per-entity world attributes to Parquet for parity checking against the
//! Python golden (golden_world.parquet).

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use datagen_rs::model::build_world;

fn main() {
    let scale: f64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0.01);
    let seed: i64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(42);
    let w = build_world(scale, seed, 60);
    let n = w.population;
    let idx = 1..=n;

    let id: Vec<i64> = idx.clone().map(|i| i as i64).collect();
    let ty: Vec<i32> = idx.clone().map(|i| w.ty[i] as i32).collect();
    let country: Vec<&str> = idx.clone().map(|i| w.country[i]).collect();
    let name: Vec<&str> = idx.clone().map(|i| w.name.get(i)).collect();
    let street: Vec<&str> = idx.clone().map(|i| w.street.get(i)).collect();
    let town: Vec<&str> = idx.clone().map(|i| w.town.get(i)).collect();
    let region: Vec<&str> = idx.clone().map(|i| w.region[i].as_str()).collect();
    let postcode: Vec<&str> = idx.clone().map(|i| w.postcode[i].as_str()).collect();
    let email: Vec<&str> = idx.clone().map(|i| w.email[i].as_str()).collect();
    let iban: Vec<&str> = idx.clone().map(|i| w.iban[i].as_str()).collect();
    let lei: Vec<&str> = idx.clone().map(|i| w.lei[i].as_str()).collect();
    let bic: Vec<&str> = idx.clone().map(|i| w.bic[i].as_str()).collect();
    let ccy: Vec<&str> = idx.clone().map(|i| w.ccy[i]).collect();
    let sanc: Vec<bool> = idx.clone().map(|i| w.sanctioned[i]).collect();
    let pep: Vec<bool> = idx.clone().map(|i| w.pep[i]).collect();
    let nacct: Vec<i32> = idx.clone().map(|i| w.n_accounts[i]).collect();

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
        Field::new("sanc", DataType::Boolean, false),
        Field::new("pep", DataType::Boolean, false),
        Field::new("nacct", DataType::Int32, false),
    ]));

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(id)),
        Arc::new(Int32Array::from(ty)),
        Arc::new(StringArray::from(country)),
        Arc::new(StringArray::from(name)),
        Arc::new(StringArray::from(street)),
        Arc::new(StringArray::from(town)),
        Arc::new(StringArray::from(region)),
        Arc::new(StringArray::from(postcode)),
        Arc::new(StringArray::from(email)),
        Arc::new(StringArray::from(iban)),
        Arc::new(StringArray::from(lei)),
        Arc::new(StringArray::from(bic)),
        Arc::new(StringArray::from(ccy)),
        Arc::new(BooleanArray::from(sanc)),
        Arc::new(BooleanArray::from(pep)),
        Arc::new(Int32Array::from(nacct)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let file = File::create("/tmp/rs_world.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    eprintln!("wrote /tmp/rs_world.parquet {} rows", n);
}
