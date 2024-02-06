use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize};
use kvs::{KvStore, KvsEngine, Sled};
use tempfile::TempDir;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::HashMap;

pub fn set_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).expect("failed to create kv store");
    let kv = rand_key_values();
    group.bench_function("kvs", |b| b.iter_batched(||{},|_|{
        for (k,v) in kv.iter() {
            store.set(k.to_string(), v.to_string()).unwrap();
        }
    },BatchSize::SmallInput));

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Sled::open(temp_dir.path()).expect("failed to create kv store");
    group.bench_function("sled", |b| b.iter_batched(||{},|_|{
        for (k,v) in kv.iter() {
            store.set(k.to_string(), v.to_string()).unwrap();
        }
    },BatchSize::SmallInput));
    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).expect("failed to create kv store");
    let kv = rand_key_values();
    for (k,v) in kv.iter() {
        store.set(k.to_string(), v.to_string()).unwrap();
    }
    group.bench_function("kvs", |b| b.iter_batched(||{},|_|{
        for (k,_) in kv.iter() {
            store.get(k.to_string()).unwrap();
        }
    },BatchSize::SmallInput));

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Sled::open(temp_dir.path()).expect("failed to create kv store");
    for (k,v) in kv.iter() {
        store.set(k.to_string(), v.to_string()).unwrap();
    }
    group.bench_function("sled", |b| b.iter_batched(||{},|_|{
        for (k,v) in kv.iter() {
            store.set(k.to_string(), v.to_string()).unwrap();
        }
    },BatchSize::SmallInput));
    group.finish();
}

criterion_group!(benches, set_benchmark, get_benchmark);
criterion_main!(benches);


fn generate_random_string(len: i32) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(len.try_into().unwrap())
        .map(char::from)
        .collect()
}

fn random_integer_in_range(min: i32, max: i32) -> i32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(min..=max)
}

fn rand_key_values()-> HashMap<String,String>{
    let mut kv: HashMap<String,String> = HashMap::new();
    for _ in 0..100 {
        let k = generate_random_string(random_integer_in_range(1, 1000));
        let v = generate_random_string(random_integer_in_range(1, 1000));
        kv.insert(k, v);
    }
    kv
}