use super::StoreCore;
use shardcache_runtime::{
    CpuTransferTarget, TransferBackend as RuntimeTransferBackend, VllmBlockAllocation,
    VllmConnectorLoadSpec, VllmRequestedPage,
};
use shardmap_crate::config::EvictionPolicy;
use shardmap_crate::cuda::CudaConfig;
use shardmap_crate::storage::EmbeddedRouteMode;

use crate::NumaRoutePolicy;

#[test]
fn service_namespace_prefixes_store_keys() {
    let (namespace, prefix) = super::normalize_service_namespace(" lmcache ");
    assert_eq!(namespace.as_deref(), Some("lmcache"));

    let prefix = prefix.expect("namespace should create a key prefix");
    assert_eq!(
        super::namespace_key(Some(prefix.as_slice()), b"token:1"),
        b"lmcache\0token:1"
    );
    assert_eq!(
        super::namespace_owned_key(Some(prefix.as_slice()), b"token:2".to_vec()),
        b"lmcache\0token:2"
    );

    let (namespace, prefix) = super::normalize_service_namespace(" ");
    assert!(namespace.is_none());
    assert!(prefix.is_none());
}

#[test]
fn lmcache_session_prefix_inherits_service_namespace() {
    assert_eq!(
        super::extract_lmcache_session_prefix(b"model@session%abc@block").as_deref(),
        Some(b"lmcache-session:abc".as_slice())
    );
    assert_eq!(
        super::extract_lmcache_session_prefix(b"lmcache\0model@session%abc@block").as_deref(),
        Some(b"lmcache\0lmcache-session:abc".as_slice())
    );
}

#[test]
fn py_store_defaults_to_resident_namespaced_service() {
    let store = super::PyStore::new(
        1,
        None,
        true,
        Some(1024),
        "lru",
        "full_key",
        false,
        "local_embedded",
        false,
        "off",
        "svc-a",
        true,
    )
    .expect("resident store should build");

    assert_eq!(store.max_memory_bytes, None);
    assert_eq!(store.eviction_policy, EvictionPolicy::None);
    assert_eq!(store.service_namespace.as_deref(), Some("svc-a"));
    assert_eq!(store.key_prefix.as_deref(), Some(b"svc-a\0".as_slice()));

    let view = store
        .with_service_namespace("svc-b", Some(true))
        .expect("resident namespace view should build");
    assert!(std::sync::Arc::ptr_eq(&store.inner, &view.inner));
    assert_eq!(view.service_namespace.as_deref(), Some("svc-b"));
    assert_eq!(view.key_prefix.as_deref(), Some(b"svc-b\0".as_slice()));
}

#[test]
fn py_store_namespace_view_cannot_change_engine_eviction_mode() {
    let store = super::PyStore::new(
        1,
        None,
        true,
        Some(1024),
        "lru",
        "full_key",
        false,
        "local_embedded",
        false,
        "off",
        "resident",
        true,
    )
    .expect("resident store should build");

    pyo3::prepare_freethreaded_python();
    let err = match store.with_service_namespace("cache", Some(false)) {
        Ok(_) => panic!("namespace view cannot change engine residency"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("eviction policy is engine-wide"),
        "unexpected error: {err}"
    );
}

#[cfg(feature = "prefix-eviction")]
#[test]
fn parses_prefix_eviction_policy_when_feature_is_enabled() {
    assert_eq!(
        super::parse_eviction_policy("prefix").expect("prefix policy should parse"),
        EvictionPolicy::Prefix
    );
    assert_eq!(
        super::parse_eviction_policy("prefix_eviction")
            .expect("prefix_eviction alias should parse"),
        EvictionPolicy::Prefix
    );
}

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
        NumaRoutePolicy::Off,
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
        NumaRoutePolicy::Off,
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
        NumaRoutePolicy::Off,
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

#[test]
fn threaded_store_routes_full_key_workloads_with_multiple_workers() {
    let core = StoreCore::new(
        2,
        None,
        true,
        None,
        EvictionPolicy::None,
        EmbeddedRouteMode::FullKey,
        false,
        "local_embedded",
        false,
        NumaRoutePolicy::Off,
    )
    .expect("threaded store should build");

    for index in 0..128usize {
        let key = format!("k:{index:04x}").into_bytes();
        let value = format!("v:{index:04x}").into_bytes();
        core.set(key.clone(), value.clone(), None);
        assert_eq!(core.get(&key), Some(value));
    }
}

#[test]
fn caller_local_numa_policy_keeps_threaded_store_operations_routed() {
    let core = StoreCore::new(
        2,
        None,
        true,
        None,
        EvictionPolicy::None,
        EmbeddedRouteMode::FullKey,
        false,
        "local_embedded",
        false,
        NumaRoutePolicy::CallerLocal,
    )
    .expect("threaded store should build");

    let key = b"session-aware-key".to_vec();
    core.set(key.clone(), b"local".to_vec(), None);
    assert!(core.exists(&key));
    assert_eq!(core.get(&key), Some(b"local".to_vec()));
    assert!(core.delete(&key));
    assert!(!core.exists(&key));
}
