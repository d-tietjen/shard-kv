//! Generate the Redis command compatibility manifest from benchmark coverage.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Write as _;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use fast_cache_benchmarks::redis_command_cases::{
    REDIS_COMMAND_CASES, REDIS_COMMAND_DESTRUCTIVE_CASES, REDIS_COMMAND_LARGE_CASES,
    RedisCommandCase,
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
}

fn render_markdown(entries: &[CommandEntry]) -> String {
    let supported = count_status(entries, CompatStatus::Supported);
    let missing = count_status(entries, CompatStatus::Missing);
    let benchmark_cases = REDIS_COMMAND_CASES.len()
        + REDIS_COMMAND_LARGE_CASES.len()
        + REDIS_COMMAND_DESTRUCTIVE_CASES.len();
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
        "`supported` means there is a Redis/Valkey-compatible implementation and at least one live RESP benchmark case. Destructive keyspace-wide cases live in the explicit `profile:destructive` matrix so they do not poison ordinary mixed runs. `missing` means it is outside the 0.2.0 compatibility surface."
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Commands").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "| Family | Command | Status | Cases | Profiles | Keyspace Wide | Notes |"
    )
    .unwrap();
    writeln!(out, "| --- | --- | --- | ---: | --- | --- | --- |").unwrap();
    for entry in entries {
        let profiles = join_set(&entry.profiles);
        let notes = notes_for(entry);
        writeln!(
            out,
            "| {} | `{}` | {} | {} | {} | {} | {} |",
            entry.family,
            entry.command,
            entry.status.label(),
            entry.cases.len(),
            markdown_cell(&profiles),
            if entry.keyspace_wide { "yes" } else { "no" },
            markdown_cell(&notes),
        )
        .unwrap();
    }
    out
}

fn render_json(entries: &[CommandEntry]) -> String {
    let supported = count_status(entries, CompatStatus::Supported);
    let missing = count_status(entries, CompatStatus::Missing);
    let benchmark_cases = REDIS_COMMAND_CASES.len()
        + REDIS_COMMAND_LARGE_CASES.len()
        + REDIS_COMMAND_DESTRUCTIVE_CASES.len();

    let mut out = String::new();
    writeln!(out, "{{").unwrap();
    writeln!(out, "  \"schema_version\": 1,").unwrap();
    writeln!(
        out,
        "  \"source\": \"benchmarks/src/redis_command_cases.rs\","
    )
    .unwrap();
    writeln!(out, "  \"summary\": {{").unwrap();
    writeln!(out, "    \"supported_commands\": {supported},").unwrap();
    writeln!(out, "    \"missing_commands\": {missing},").unwrap();
    writeln!(out, "    \"benchmark_cases\": {benchmark_cases},").unwrap();
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
    format!("Benchmark cases: {}", join_set(&entry.cases))
}

fn join_set(values: &BTreeSet<&'static str>) -> String {
    values.iter().copied().collect::<Vec<_>>().join(", ")
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
    use super::{CompatStatus, build_manifest, count_status};

    #[test]
    fn manifest_counts_are_intentional() {
        let entries = build_manifest();
        assert_eq!(count_status(&entries, CompatStatus::Supported), 155);
        assert_eq!(count_status(&entries, CompatStatus::Missing), 0);
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
}
