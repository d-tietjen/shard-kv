//! Isolated Linux protection probes. No global settings or other processes change.
#[cfg(target_os = "linux")]
mod linux {
    use std::error::Error;
    use std::os::unix::fs::MetadataExt;
    type Result<T> = std::result::Result<T, Box<dyn Error>>;

    fn references(device: u64, inode: u64) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let metadata = std::fs::metadata(entry.ok()?.path()).ok()?;
                Some(usize::from(
                    metadata.dev() == device && metadata.ino() == inode,
                ))
            })
            .sum()
    }
    fn default_heap() -> Result<()> {
        // SAFETY: read-only process query, no pointer arguments.
        let before = unsafe { libc::prctl(libc::PR_GET_DUMPABLE) };
        let map = shardmap::ShardMap::<String, String>::new();
        map.insert("key".into(), "value".into());
        assert_eq!(map.get("key").as_deref(), Some("value"));
        // SAFETY: same read-only query after ordinary heap operations.
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE) }, before);
        assert!(!std::fs::read_to_string("/proc/self/maps")?.contains("memfd:shardmap-succession"));
        println!("default heap: no retained backing or process-policy change");
        Ok(())
    }
    pub fn main() -> Result<()> {
        let args: Vec<_> = std::env::args().collect();
        let mode = args.get(1).map(String::as_str).unwrap_or("default");
        if mode == "default" {
            return default_heap();
        }
        if mode == "inspect-exec" {
            assert_eq!(references(args[2].parse()?, args[3].parse()?), 0);
            return Ok(());
        }
        #[cfg(feature = "succession")]
        return protected(mode);
        #[cfg(not(feature = "succession"))]
        Err("protection probes require succession".into())
    }

    #[cfg(feature = "succession")]
    fn protected(mode: &str) -> Result<()> {
        use shardmap::succession::{
            Cache, Error as CacheError, MemoryPolicy, Options, protect_current_process,
        };
        protect_current_process()?;
        let options = Options {
            capacity: 8,
            max_key_bytes: 32,
            max_value_bytes: 128,
            default_ttl_ms: None,
        };
        if mode == "locked-denied" || mode == "pageable-zero-limit" {
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: isolated fixture process; never changes another process.
            if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &limit) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if mode == "locked-denied" {
                assert!(matches!(
                    Cache::create_with_memory_policy(options, MemoryPolicy::Locked),
                    Err(CacheError::MemoryLock(_))
                ));
                assert!(
                    !std::fs::read_to_string("/proc/self/maps")?
                        .contains("memfd:shardmap-succession")
                );
                return Ok(());
            }
        }
        if mode == "deny-madvise" || mode == "deny-dontfork" {
            // A narrowly scoped fault injection: this child denies madvise only.
            // All other syscalls remain available for cleanup and test output.
            let mut filter = [
                libc::sock_filter {
                    code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                    jt: 0,
                    jf: 0,
                    k: 0,
                },
                libc::sock_filter {
                    code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
                    jt: 0,
                    jf: 3,
                    k: libc::SYS_madvise as u32,
                },
                libc::sock_filter {
                    code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                    jt: 0,
                    jf: 0,
                    k: 32, // seccomp_data.args[2]: advice
                },
                libc::sock_filter {
                    code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
                    jt: 0,
                    jf: 1,
                    k: if mode == "deny-dontfork" {
                        libc::MADV_DONTFORK
                    } else {
                        libc::MADV_DONTDUMP
                    } as u32,
                },
                libc::sock_filter {
                    code: (libc::BPF_RET | libc::BPF_K) as u16,
                    jt: 0,
                    jf: 0,
                    k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
                },
                libc::sock_filter {
                    code: (libc::BPF_RET | libc::BPF_K) as u16,
                    jt: 0,
                    jf: 0,
                    k: libc::SECCOMP_RET_ALLOW,
                },
            ];
            let program = libc::sock_fprog {
                len: filter.len() as u16,
                filter: filter.as_mut_ptr(),
            };
            // SAFETY: bounded, live filter; confinement only of this disposable child.
            if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0
                || unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) }
                    != 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            assert!(
                matches!(Cache::create(options), Err(CacheError::Io(error)) if error.raw_os_error() == Some(libc::EPERM))
            );
            assert!(
                !std::fs::read_to_string("/proc/self/maps")?.contains("memfd:shardmap-succession")
            );
            assert!(
                std::fs::read_dir("/proc/self/fd")?
                    .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
                    .all(|p| !p.to_string_lossy().contains("memfd:shardmap-succession"))
            );
            return Ok(());
        }
        let policy = if mode == "locked" {
            MemoryPolicy::Locked
        } else {
            MemoryPolicy::Pageable
        };
        let cache = Cache::create_with_memory_policy(options, policy)?;
        cache.insert(b"key", b"retained", None)?;
        let base = cache.mapping_address();
        let smaps = std::fs::read_to_string("/proc/self/smaps")?;
        let mut selected = false;
        let mut flags = Vec::new();
        for line in smaps.lines() {
            if line
                .split_whitespace()
                .next()
                .is_some_and(|word| word.contains('-'))
            {
                selected = line.starts_with(&format!("{base:x}-"));
            }
            if selected && line.starts_with("VmFlags:") {
                flags = line.split_whitespace().map(str::to_owned).collect();
                break;
            }
        }
        assert!(flags.iter().any(|flag| flag == "dd"));
        assert!(flags.iter().any(|flag| flag == "dc"));
        assert_eq!(
            flags.iter().any(|flag| flag == "lo"),
            policy == MemoryPolicy::Locked
        );
        let identity = cache.identity()?;
        if mode == "fork" {
            assert_eq!(std::fs::read_dir("/proc/self/task")?.count(), 1);
            // SAFETY: restricted single-threaded probe. The child uses only raw
            // syscalls and _exit, never Rust cache APIs or inherited allocators.
            let pid = unsafe { libc::fork() };
            if pid < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if pid == 0 {
                let mut resident = 0u8;
                // SAFETY: mincore observes whether this range exists; it does not
                // dereference the missing mapping or mutate the parent's mapping.
                let result =
                    unsafe { libc::syscall(libc::SYS_mincore, base, 1usize, &mut resident) };
                unsafe {
                    libc::_exit(
                        if result == -1 && *libc::__errno_location() == libc::ENOMEM {
                            0
                        } else {
                            3
                        },
                    );
                }
            }
            let mut status = 0;
            // SAFETY: wait only for this owned child and writable status.
            assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
            assert_eq!(status, 0);
        }
        if mode == "exec" {
            let status = std::process::Command::new(std::env::current_exe()?)
                .arg("inspect-exec")
                .arg(identity.device.to_string())
                .arg(identity.inode.to_string())
                .status()?;
            assert!(status.success());
        }
        assert_eq!(
            &*cache.get(b"key")?.ok_or("parent retained data")?,
            b"retained"
        );
        println!("{mode}: protected mapping, policy {policy:?}, retained parent data");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    return linux::main();
    #[cfg(not(target_os = "linux"))]
    Ok(())
}
