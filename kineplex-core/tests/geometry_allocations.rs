use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;

use kineplex_core::geometry::{
    metric_norm_squared, AtomicGeodesicTable, HomotopyPath, MetricSample, MetricTensor, RouteEntry,
    RouteSnapshot,
};

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

struct CountingSystem;

unsafe impl GlobalAlloc for CountingSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the allocator contract to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        count_if_tracking(!pointer.is_null());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwards the allocator contract to the system allocator.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        count_if_tracking(!pointer.is_null());
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forwards the allocator contract to the system allocator.
        let result = unsafe { System.realloc(pointer, layout, size) };
        count_if_tracking(!result.is_null());
        result
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: pointer and layout are passed through unchanged.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingSystem = CountingSystem;

fn count_if_tracking(allocated: bool) {
    if !allocated {
        return;
    }
    let _tracking = TRACK_ALLOCATIONS.try_with(|enabled| {
        if enabled.get() {
            let _count = ALLOCATION_COUNT.try_with(|count| count.set(count.get() + 1));
        }
    });
}

#[test]
fn geometry_lookup_and_interpolation_allocate_no_heap_memory() {
    let routes = AtomicGeodesicTable::new(RouteSnapshot {
        revision: 1,
        routes: HashMap::from([(
            4,
            RouteEntry {
                next_node: 7,
                resistance: 0.5,
            },
        )]),
    });
    let metric = MetricTensor::from_sample(
        MetricSample {
            latency_ms: 1.0,
            queued_bytes: 0.0,
            cpu_percent: 10.0,
        },
        1,
    );
    let path = HomotopyPath::new(vec![[0.0; 3]], vec![[1.0; 3]]).expect("path is valid");
    let mut position = [[0.0; 3]; 1];
    ALLOCATION_COUNT.with(|count| count.set(0));
    TRACK_ALLOCATIONS.with(|enabled| enabled.set(true));
    for _ in 0..10_000 {
        let _route = routes.lookup(4);
        let _norm = metric_norm_squared(&metric, [1.0, 2.0, 3.0]);
        let _sample = path.sample_into(0.5, &mut position);
    }
    TRACK_ALLOCATIONS.with(|enabled| enabled.set(false));
    let allocations = ALLOCATION_COUNT.with(Cell::get);
    assert_eq!(allocations, 0, "hot path allocated {allocations} times");
}
