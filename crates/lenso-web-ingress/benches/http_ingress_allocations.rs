use std::alloc::System;

use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

mod benchmark {
    include!("http_ingress.rs");
}

struct AllocationObserver {
    region: Region<'static, System>,
}

impl benchmark::MeasurementObserver for AllocationObserver {
    fn before_measure(&mut self) {
        self.region.reset();
    }

    fn after_measure(
        &mut self,
        server: &str,
        request_body_size: usize,
        connections: usize,
        requests: usize,
    ) {
        let stats = self.region.change();
        println!(
            "allocation_sample\tserver={server}\trequest_body_bytes={request_body_size}\tconnections={connections}\trequests={requests}\tallocations={}\tdeallocations={}\tbytes_allocated={}\tbytes_deallocated={}\tbytes_reallocated={}",
            stats.allocations,
            stats.deallocations,
            stats.bytes_allocated,
            stats.bytes_deallocated,
            stats.bytes_reallocated,
        );
    }
}

fn main() {
    std::hint::black_box(benchmark::main as fn());
    benchmark::main_with_observer(&mut AllocationObserver {
        region: Region::new(GLOBAL),
    });
}
