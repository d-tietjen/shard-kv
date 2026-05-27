use super::StoreCore;
use fast_cache_crate::config::EvictionPolicy;
use fast_cache_crate::cuda::CudaConfig;
use fast_cache_crate::storage::EmbeddedRouteMode;
use fast_cache_runtime::{
    CpuTransferTarget, TransferBackend as RuntimeTransferBackend, VllmBlockAllocation,
    VllmConnectorLoadSpec, VllmRequestedPage,
};

#[test]
fn threaded_store_can_restore_vllm_pages_with_cpu_fallback() {
    let core = StoreCore::new(
        1,
        None,
        true,
        None,
        EvictionPolicy::None,
        EmbeddedRouteMode::SessionPrefix,
        false,
        "local_embedded",
        false,
    )
    .expect("threaded store should build");
    core.batch_set_session_owned_no_ttl(
        b"s:1".to_vec(),
        vec![
            (b"k0".to_vec(), b"abcd".to_vec()),
            (b"k1".to_vec(), b"wxyz".to_vec()),
        ],
    );

    let spec = VllmConnectorLoadSpec::new(
        b"s:1".to_vec(),
        vec![
            VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 4),
            VllmRequestedPage::new(b"k1".to_vec(), 1, 1, 4),
        ],
        vec![
            VllmBlockAllocation::new(0, 0x1000, 4),
            VllmBlockAllocation::new(1, 0x2000, 4),
        ],
    )
    .with_gpu_target(0, 0)
    .with_allow_cpu_fallback(true);

    let mut dst = vec![0u8; 8];
    let report = core
        .restore_vllm_paged(
            spec,
            CudaConfig {
                enabled: false,
                allow_cpu_fallback: true,
                ..CudaConfig::default()
            },
            Some(CpuTransferTarget {
                allocation_id: 7,
                dst_host_ptr: dst.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
        )
        .expect("restore should succeed through cpu fallback");

    assert_eq!(report.backend, RuntimeTransferBackend::Cpu);
    assert_eq!(report.page_count, 2);
    assert_eq!(report.hit_pages, 2);
    assert_eq!(report.missed_pages, 0);
    assert_eq!(report.transferred_bytes, 8);
    assert!(report.all_hit);
    assert_eq!(report.total_expected_bytes, Some(8));
    assert_eq!(dst, b"abcdwxyz");
}

#[test]
fn threaded_store_can_restore_vllm_pages_without_gpu_blocks_when_cpu_fallback_exists() {
    let core = StoreCore::new(
        1,
        None,
        true,
        None,
        EvictionPolicy::None,
        EmbeddedRouteMode::SessionPrefix,
        false,
        "local_embedded",
        false,
    )
    .expect("threaded store should build");
    core.batch_set_session_owned_no_ttl(
        b"s:cpu".to_vec(),
        vec![
            (b"k0".to_vec(), b"abcd".to_vec()),
            (b"k1".to_vec(), b"wxyz".to_vec()),
        ],
    );

    let spec = VllmConnectorLoadSpec::new(
        b"s:cpu".to_vec(),
        vec![
            VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 4),
            VllmRequestedPage::new(b"k1".to_vec(), 1, 1, 4),
        ],
        Vec::new(),
    )
    .with_allow_cpu_fallback(true);

    let mut dst = vec![0u8; 8];
    let report = core
        .restore_vllm_paged(
            spec,
            CudaConfig {
                enabled: true,
                allow_cpu_fallback: true,
                ..CudaConfig::default()
            },
            Some(CpuTransferTarget {
                allocation_id: 9,
                dst_host_ptr: dst.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
        )
        .expect("restore should succeed through direct cpu fallback");

    assert_eq!(report.backend, RuntimeTransferBackend::Cpu);
    assert_eq!(report.page_count, 2);
    assert_eq!(report.hit_pages, 2);
    assert_eq!(report.missed_pages, 0);
    assert_eq!(report.transferred_bytes, 8);
    assert!(report.all_hit);
    assert_eq!(report.total_expected_bytes, Some(8));
    assert_eq!(dst, b"abcdwxyz");
}

#[test]
fn shared_store_rejects_direct_vllm_restore() {
    let core = StoreCore::new(
        1,
        None,
        true,
        None,
        EvictionPolicy::None,
        EmbeddedRouteMode::SessionPrefix,
        false,
        "shared",
        false,
    )
    .expect("shared store should build");

    let err = core
        .restore_vllm_paged(
            VllmConnectorLoadSpec::new(
                b"s:1".to_vec(),
                vec![VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 4)],
                vec![VllmBlockAllocation::new(0, 0x1000, 4)],
            )
            .with_gpu_target(0, 0),
            CudaConfig::default(),
            None,
        )
        .expect_err("shared store should reject direct vllm restore");

    assert!(
        err.to_string()
            .contains("client_architecture='local_embedded'"),
        "unexpected error: {err}"
    );
}
