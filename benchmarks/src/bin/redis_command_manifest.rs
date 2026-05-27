//! Generate the Redis command compatibility manifest from benchmark coverage.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Write as _;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use fast_cache_benchmarks::redis_command_cases::{
    REDIS_5_0_14_COMMANDS, REDIS_5_0_14_EXCLUSIONS, REDIS_COMMAND_CASES,
    REDIS_COMMAND_DESTRUCTIVE_CASES, REDIS_COMMAND_LARGE_CASES, RedisCommandCase,
};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Generate Redis command compatibility docs or JSON")]
struct Args {
    /// Output format.
    #[arg(long, default_value = "markdown")]
    format: OutputFormat,

    /// Optional output path. Writes to stdout when omitted.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum OutputFormat {
    Markdown,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CompatStatus {
    Supported,
    Missing,
}

impl CompatStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone)]
struct CommandEntry {
    family: &'static str,
    command: &'static str,
    status: CompatStatus,
    cases: BTreeSet<&'static str>,
    profiles: BTreeSet<&'static str>,
    keyspace_wide: bool,
    expected_error: bool,
    notes: &'static str,
}

impl CommandEntry {
    fn supported(family: &'static str, command: &'static str) -> Self {
        Self {
            family,
            command,
            status: CompatStatus::Supported,
            cases: BTreeSet::new(),
            profiles: BTreeSet::new(),
            keyspace_wide: false,
            expected_error: false,
            notes: "",
        }
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let entries = build_manifest();
    let output = match args.format {
        OutputFormat::Markdown => render_markdown(&entries),
        OutputFormat::Json => render_json(&entries),
    };

    match args.output {
        Some(path) => std::fs::write(path, output)?,
        None => print!("{output}"),
    }
    Ok(())
}

fn build_manifest() -> Vec<CommandEntry> {
    let mut entries = BTreeMap::<&'static str, CommandEntry>::new();
    for case in REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
    {
        add_case(&mut entries, case);
    }

    entries.into_values().collect()
}

fn add_case(entries: &mut BTreeMap<&'static str, CommandEntry>, case: &RedisCommandCase) {
    let entry = entries
        .entry(case.command_name)
        .or_insert_with(|| CommandEntry::supported(case.family.label(), case.command_name));
    entry.cases.insert(case.case_name);
    entry.profiles.insert(case.profile.label());
    entry.keyspace_wide |= case.is_keyspace_wide();
    entry.expected_error |= case.expect_error;
}

fn render_markdown(entries: &[CommandEntry]) -> String {
    let supported = count_status(entries, CompatStatus::Supported);
    let missing = count_status(entries, CompatStatus::Missing);
    let redis5_supported = redis_5_supported_commands(entries);
    let redis5_excluded = redis_5_excluded_commands();
    let redis5_missing = redis_5_missing_commands(entries);
    let redis5_extensions = redis_5_extension_commands(entries);
    let benchmark_cases = REDIS_COMMAND_CASES.len()
        + REDIS_COMMAND_LARGE_CASES.len()
        + REDIS_COMMAND_DESTRUCTIVE_CASES.len();
    let expected_error_cases = REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
        .filter(|case| case.expect_error)
        .count();
    let keyspace_cases = REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
        .filter(|case| case.is_keyspace_wide())
        .count();

    let mut out = String::new();
    writeln!(out, "# Redis Compatibility Manifest").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Generated from `benchmarks/src/redis_command_cases.rs`. Keep this file fresh with:"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "```bash\ncargo run -p fast-cache-benchmarks --bin redis_command_manifest -- --output docs/REDIS_COMPATIBILITY.md\n```"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Metric | Count |").unwrap();
    writeln!(out, "| --- | ---: |").unwrap();
    writeln!(out, "| Supported commands | {supported} |").unwrap();
    writeln!(out, "| Missing commands | {missing} |").unwrap();
    writeln!(out, "| Live benchmark cases | {benchmark_cases} |").unwrap();
    writeln!(
        out,
        "| Expected-error benchmark cases | {expected_error_cases} |"
    )
    .unwrap();
    writeln!(
        out,
        "| Large-profile cases | {} |",
        REDIS_COMMAND_LARGE_CASES.len()
    )
    .unwrap();
    writeln!(
        out,
        "| Destructive-profile cases | {} |",
        REDIS_COMMAND_DESTRUCTIVE_CASES.len()
    )
    .unwrap();
    writeln!(out, "| Keyspace-wide benchmark cases | {keyspace_cases} |").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "`supported` means there is a Redis/Valkey-compatible implementation and at least one live RESP benchmark case. Expected-error cases are commands whose Redis-compatible behavior in fast-cache's standalone mode is an error reply, such as disabled cluster, replication, monitor, shutdown, or security-warning commands. Destructive keyspace-wide cases live in the explicit `profile:destructive` matrix so they do not poison ordinary mixed runs. `missing` means it is outside the 0.2.0 compatibility surface."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Redis 5.0.14 Baseline").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "Official baseline: Redis 5.0.14 `redisCommandTable` from <https://github.com/redis/redis/blob/5.0.14/src/server.c>."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| Metric | Count |").unwrap();
    writeln!(out, "| --- | ---: |").unwrap();
    writeln!(
        out,
        "| Redis 5.0.14 command table entries | {} |",
        REDIS_5_0_14_COMMANDS.len()
    )
    .unwrap();
    writeln!(
        out,
        "| Redis 5.0.14 commands supported and live-benchmarked | {} |",
        redis5_supported.len()
    )
    .unwrap();
    writeln!(
        out,
        "| Redis 5.0.14 commands explicitly excluded from 0.2.0 | {} |",
        redis5_excluded.len()
    )
    .unwrap();
    writeln!(
        out,
        "| Redis 5.0.14 commands missing | {} |",
        redis5_missing.len()
    )
    .unwrap();
    writeln!(
        out,
        "| Supported extensions beyond Redis 5.0.14 | {} |",
        redis5_extensions.len()
    )
    .unwrap();
    writeln!(out).unwrap();
    if redis5_excluded.is_empty() {
        writeln!(
            out,
            "No Redis 5.0.14 commands are excluded from the compatibility target. Redis 5.0.14 commands that are not supported yet are tracked as missing compatibility work."
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "Explicit exclusions are outside the current compatibility target. Redis 5.0.14 commands that are not supported yet are tracked as missing compatibility work."
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    if redis5_missing.is_empty() {
        writeln!(out, "Missing Redis 5.0.14 commands: none.").unwrap();
    } else {
        writeln!(
            out,
            "Missing Redis 5.0.14 commands: {}.",
            join_set_as_code(&redis5_missing)
        )
        .unwrap();
    }
    writeln!(out).unwrap();
    writeln!(
        out,
        "Supported extensions beyond Redis 5.0.14: {}.",
        join_set_as_code(&redis5_extensions)
    )
    .unwrap();
    writeln!(out).unwrap();
    if redis5_excluded.is_empty() {
        writeln!(out, "Explicit Redis 5.0.14 exclusions: none.").unwrap();
    } else {
        writeln!(out, "### Explicit Redis 5.0.14 Exclusions").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "| Command | Family | Reason |").unwrap();
        writeln!(out, "| --- | --- | --- |").unwrap();
        for exclusion in REDIS_5_0_14_EXCLUSIONS {
            writeln!(
                out,
                "| `{}` | {} | {} |",
                exclusion.command,
                exclusion.family,
                markdown_cell(exclusion.reason),
            )
            .unwrap();
        }
    }
    render_semantic_notes(&mut out);
    writeln!(out).unwrap();
    writeln!(out, "## Commands").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "| Family | Command | Status | Cases | Profiles | Keyspace Wide | Expected Error | Notes |"
    )
    .unwrap();
    writeln!(out, "| --- | --- | --- | ---: | --- | --- | --- | --- |").unwrap();
    for entry in entries {
        let profiles = join_set(&entry.profiles);
        let notes = notes_for(entry);
        writeln!(
            out,
            "| {} | `{}` | {} | {} | {} | {} | {} | {} |",
            entry.family,
            entry.command,
            entry.status.label(),
            entry.cases.len(),
            markdown_cell(&profiles),
            if entry.keyspace_wide { "yes" } else { "no" },
            if entry.expected_error { "yes" } else { "no" },
            markdown_cell(&notes),
        )
        .unwrap();
    }
    out
}

fn render_semantic_notes(out: &mut String) {
    writeln!(out).unwrap();
    writeln!(out, "## Semantic Compatibility Notes").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "- The manifest tracks live RESP command acceptance and benchmark coverage, not a promise that every edge case, exact error string, or background subsystem is byte-for-byte identical to Redis."
    )
    .unwrap();
    writeln!(
        out,
        "- Expected-error commands are part of the compatibility surface in standalone mode. They intentionally return Redis-style errors for disabled cluster, replication, monitor, shutdown, module loading, migration, cross-DB movement, and security-warning paths."
    )
    .unwrap();
    writeln!(
        out,
        "- Pub/Sub coverage currently validates publish-without-subscribers, subscription acknowledgements, unsubscribe acknowledgements, and empty introspection. Persistent subscriber fanout is not part of the 0.2.0 compatibility semantics."
    )
    .unwrap();
    writeln!(
        out,
        "- Stream coverage includes basic append, length, range, reverse range, delete, trim, set-id, read, and minimal group/readgroup paths. Pending-entry-list, claim, ack, and detailed group/consumer introspection behavior is intentionally lightweight."
    )
    .unwrap();
    writeln!(
        out,
        "- Scripting uses a constrained evaluator for return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. It is not a general Lua VM."
    )
    .unwrap();
    writeln!(
        out,
        "- HyperLogLog commands return compatible cardinalities for the covered operations, but fast-cache stores exact sets in its own representation rather than Redis' binary HLL encoding."
    )
    .unwrap();
    writeln!(
        out,
        "- Blocking list and sorted-set commands are live-tested on ready or short-timeout paths. Long-lived blocking wakeups across clients need separate proofing before being described as full Redis parity."
    )
    .unwrap();
    writeln!(
        out,
        "- FCNP one-byte opcodes cover the hot command set. Commands outside that compact opcode table use the RESP/FCNP command-name fallback path so the server can still route and execute them."
    )
    .unwrap();
}

fn render_json(entries: &[CommandEntry]) -> String {
    let supported = count_status(entries, CompatStatus::Supported);
    let missing = count_status(entries, CompatStatus::Missing);
    let redis5_supported = redis_5_supported_commands(entries);
    let redis5_excluded = redis_5_excluded_commands();
    let redis5_missing = redis_5_missing_commands(entries);
    let redis5_extensions = redis_5_extension_commands(entries);
    let benchmark_cases = REDIS_COMMAND_CASES.len()
        + REDIS_COMMAND_LARGE_CASES.len()
        + REDIS_COMMAND_DESTRUCTIVE_CASES.len();
    let expected_error_cases = REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
        .filter(|case| case.expect_error)
        .count();

    let mut out = String::new();
    writeln!(out, "{{").unwrap();
    writeln!(out, "  \"schema_version\": 2,").unwrap();
    writeln!(
        out,
        "  \"source\": \"benchmarks/src/redis_command_cases.rs\","
    )
    .unwrap();
    writeln!(out, "  \"summary\": {{").unwrap();
    writeln!(out, "    \"supported_commands\": {supported},").unwrap();
    writeln!(out, "    \"missing_commands\": {missing},").unwrap();
    writeln!(out, "    \"benchmark_cases\": {benchmark_cases},").unwrap();
    writeln!(out, "    \"expected_error_cases\": {expected_error_cases},").unwrap();
    writeln!(
        out,
        "    \"large_cases\": {},",
        REDIS_COMMAND_LARGE_CASES.len()
    )
    .unwrap();
    writeln!(
        out,
        "    \"destructive_cases\": {}",
        REDIS_COMMAND_DESTRUCTIVE_CASES.len()
    )
    .unwrap();
    writeln!(out, "  }},").unwrap();
    writeln!(out, "  \"redis_5_0_14\": {{").unwrap();
    writeln!(
        out,
        "    \"source\": \"https://github.com/redis/redis/blob/5.0.14/src/server.c\","
    )
    .unwrap();
    writeln!(
        out,
        "    \"command_count\": {},",
        REDIS_5_0_14_COMMANDS.len()
    )
    .unwrap();
    writeln!(
        out,
        "    \"supported_commands\": {},",
        redis5_supported.len()
    )
    .unwrap();
    writeln!(out, "    \"excluded_commands\": {},", redis5_excluded.len()).unwrap();
    writeln!(
        out,
        "    \"missing_commands\": [{}],",
        render_json_string_array(&redis5_missing)
    )
    .unwrap();
    writeln!(
        out,
        "    \"extensions_beyond_redis_5\": [{}],",
        render_json_string_array(&redis5_extensions)
    )
    .unwrap();
    writeln!(out, "    \"exclusions\": [").unwrap();
    for (index, exclusion) in REDIS_5_0_14_EXCLUSIONS.iter().enumerate() {
        let comma = if index + 1 == REDIS_5_0_14_EXCLUSIONS.len() {
            ""
        } else {
            ","
        };
        writeln!(out, "      {{").unwrap();
        writeln!(
            out,
            "        \"command\": \"{}\",",
            json_escape(exclusion.command)
        )
        .unwrap();
        writeln!(
            out,
            "        \"family\": \"{}\",",
            json_escape(exclusion.family)
        )
        .unwrap();
        writeln!(
            out,
            "        \"reason\": \"{}\"",
            json_escape(exclusion.reason)
        )
        .unwrap();
        writeln!(out, "      }}{comma}").unwrap();
    }
    writeln!(out, "    ]").unwrap();
    writeln!(out, "  }},").unwrap();
    writeln!(out, "  \"commands\": [").unwrap();
    for (index, entry) in entries.iter().enumerate() {
        let comma = if index + 1 == entries.len() { "" } else { "," };
        writeln!(out, "    {{").unwrap();
        writeln!(out, "      \"family\": \"{}\",", json_escape(entry.family)).unwrap();
        writeln!(
            out,
            "      \"command\": \"{}\",",
            json_escape(entry.command)
        )
        .unwrap();
        writeln!(out, "      \"status\": \"{}\",", entry.status.label()).unwrap();
        writeln!(out, "      \"case_count\": {},", entry.cases.len()).unwrap();
        writeln!(
            out,
            "      \"profiles\": [{}],",
            render_json_string_array(&entry.profiles)
        )
        .unwrap();
        writeln!(out, "      \"keyspace_wide\": {},", entry.keyspace_wide).unwrap();
        writeln!(out, "      \"expected_error\": {},", entry.expected_error).unwrap();
        writeln!(
            out,
            "      \"cases\": [{}],",
            render_json_string_array(&entry.cases)
        )
        .unwrap();
        writeln!(
            out,
            "      \"notes\": \"{}\"",
            json_escape(&notes_for(entry))
        )
        .unwrap();
        writeln!(out, "    }}{comma}").unwrap();
    }
    writeln!(out, "  ]").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn count_status(entries: &[CommandEntry], status: CompatStatus) -> usize {
    entries
        .iter()
        .filter(|entry| entry.status == status)
        .count()
}

fn redis_5_supported_commands(entries: &[CommandEntry]) -> BTreeSet<&'static str> {
    let supported = entries
        .iter()
        .filter(|entry| entry.status == CompatStatus::Supported)
        .map(|entry| entry.command)
        .collect::<BTreeSet<_>>();
    REDIS_5_0_14_COMMANDS
        .iter()
        .copied()
        .filter(|command| supported.contains(command))
        .collect()
}

fn redis_5_excluded_commands() -> BTreeSet<&'static str> {
    REDIS_5_0_14_EXCLUSIONS
        .iter()
        .map(|entry| entry.command)
        .collect()
}

fn redis_5_missing_commands(entries: &[CommandEntry]) -> BTreeSet<&'static str> {
    let supported = redis_5_supported_commands(entries);
    let excluded = redis_5_excluded_commands();
    REDIS_5_0_14_COMMANDS
        .iter()
        .copied()
        .filter(|command| !supported.contains(command) && !excluded.contains(command))
        .collect()
}

fn redis_5_extension_commands(entries: &[CommandEntry]) -> BTreeSet<&'static str> {
    let redis5 = REDIS_5_0_14_COMMANDS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    entries
        .iter()
        .filter(|entry| entry.status == CompatStatus::Supported)
        .map(|entry| entry.command)
        .filter(|command| !redis5.contains(command))
        .collect()
}

fn notes_for(entry: &CommandEntry) -> String {
    if !entry.notes.is_empty() {
        return entry.notes.to_string();
    }
    if entry.cases.is_empty() {
        return String::new();
    }
    if entry.profiles.contains("destructive") {
        return format!(
            "Destructive perf matrix case; run separately with `CASES=profile:destructive`. Benchmark cases: {}",
            join_set(&entry.cases)
        );
    }
    if entry.expected_error {
        return format!(
            "Expected RESP error reply in standalone compatibility mode. Benchmark cases: {}",
            join_set(&entry.cases)
        );
    }
    if entry.family == "scripting" {
        return format!(
            "Constrained scripting evaluator: return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. Benchmark cases: {}",
            join_set(&entry.cases)
        );
    }
    format!("Benchmark cases: {}", join_set(&entry.cases))
}

fn join_set(values: &BTreeSet<&'static str>) -> String {
    values.iter().copied().collect::<Vec<_>>().join(", ")
}

fn join_set_as_code(values: &BTreeSet<&'static str>) -> String {
    values
        .iter()
        .map(|value| format!("`{value}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|")
}

fn render_json_string_array(values: &BTreeSet<&'static str>) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other => escaped.push(other),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::{
        CompatStatus, build_manifest, count_status, redis_5_extension_commands,
        redis_5_missing_commands, redis_5_supported_commands,
    };

    #[test]
    fn manifest_counts_are_intentional() {
        let entries = build_manifest();
        assert_eq!(count_status(&entries, CompatStatus::Supported), 222);
        assert_eq!(count_status(&entries, CompatStatus::Missing), 0);
    }

    #[test]
    fn redis_5_baseline_counts_are_intentional() {
        let entries = build_manifest();
        assert_eq!(redis_5_supported_commands(&entries).len(), 200);
        assert_eq!(redis_5_missing_commands(&entries).len(), 0);
        assert_eq!(redis_5_extension_commands(&entries).len(), 22);
    }

    #[test]
    fn manifest_marks_known_keyspace_commands() {
        let entries = build_manifest();
        assert!(
            entries
                .iter()
                .find(|entry| entry.command == "KEYS")
                .is_some_and(|entry| entry.keyspace_wide)
        );
        assert!(
            entries
                .iter()
                .find(|entry| entry.command == "SCAN")
                .is_some_and(|entry| entry.keyspace_wide)
        );
    }

    #[test]
    fn manifest_marks_expected_error_commands() {
        let entries = build_manifest();
        let expected_error = entries
            .iter()
            .filter(|entry| entry.expected_error)
            .map(|entry| entry.command)
            .collect::<Vec<_>>();

        assert_eq!(
            expected_error,
            [
                "CLUSTER", "HOST:", "MIGRATE", "MONITOR", "MOVE", "POST", "PSYNC", "SHUTDOWN",
                "SYNC",
            ]
        );
    }
}
