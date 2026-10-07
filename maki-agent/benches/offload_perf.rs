use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use maki_agent::tools::offload::{OffloadError, OffloadStore, PutOutcome};

mod offload_benchmark_fixtures {
    include!("../src/tools/offload_benchmark_fixtures.rs");
}

const ARTIFACT_COUNTS: [usize; 3] = [10, 100, 1000];

fn bench_offload(c: &mut Criterion) {
    let mut group = c.benchmark_group("offload_put");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_secs(1));

    for artifact_count in ARTIFACT_COUNTS {
        group.bench_function(format!("new/{artifact_count}"), |b| {
            b.iter_batched_ref(
                || offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap(),
                |fixture| fixture.put_new().unwrap(),
                BatchSize::SmallInput,
            );
        });
        group.bench_function(format!("identical/{artifact_count}"), |b| {
            b.iter_batched_ref(
                || offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap(),
                |fixture| fixture.put_duplicate().unwrap(),
                BatchSize::SmallInput,
            );
        });
        group.bench_function(format!("sequence/{artifact_count}"), |b| {
            b.iter_batched_ref(
                || offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap(),
                |fixture| fixture.put_sequence().unwrap(),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_offload);
criterion_main!(benches);
