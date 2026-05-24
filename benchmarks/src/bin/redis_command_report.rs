//! Summarize a redis_command_matrix CSV into Markdown and JSON artifacts.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use clap::Parser;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Generate a report bundle from redis_command_matrix CSV")]
struct Args {
    /// CSV emitted by redis_command_matrix.
    #[arg(long)]
    csv: PathBuf,

    /// Saved Redis/Valkey/Dragonfly CSV to merge into the report.
    ///
    /// Reference rows are loaded before the primary CSV, so current-run rows
    /// replace saved rows for the same target/client/command case.
    #[arg(long = "reference-csv")]
    reference_csvs: Vec<PathBuf>,

    /// Markdown report output path.
    #[arg(long)]
    markdown: Option<PathBuf>,

    /// JSON summary output path.
    #[arg(long)]
    json: Option<PathBuf>,

    /// Baseline target for ratio calculations. Defaults to the first target in the CSV.
    #[arg(long)]
    baseline: Option<String>,

    /// Human-readable run label.
    #[arg(long, default_value = "redis command matrix")]
    label: String,

    /// Number of slowest cases to include per target in Markdown.
    #[arg(long, default_value_t = 10)]
    slowest: usize,
}

#[derive(Debug, Clone)]
struct CsvRow {
    target: String,
    family: String,
    command: String,
    case_name: String,
    clients: usize,
    duration_s: u64,
    ops: u64,
    ops_per_sec: f64,
    avg_us: f64,
    errors: u64,
    profile: String,
}

#[derive(Debug, Clone, Default)]
struct TargetSummary {
    target: String,
    cases: usize,
    clients: usize,
    duration_s: u64,
    total_ops: u64,
    sum_ops_per_sec: f64,
    mean_avg_us: f64,
    errors: u64,
    small_cases: usize,
    large_cases: usize,
    rows: Vec<CsvRow>,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let primary_rows = read_rows(&args.csv)?;
    if primary_rows.is_empty() {
        return Err(format!("{} contained no data rows", args.csv.display()).into());
    }
    let default_baseline = primary_rows[0].target.clone();

    let mut reference_rows = Vec::new();
    for path in &args.reference_csvs {
        let mut rows = read_rows(path)?;
        if rows.is_empty() {
            return Err(format!("{} contained no data rows", path.display()).into());
        }
        reference_rows.append(&mut rows);
    }

    let rows = merge_rows(primary_rows, reference_rows);
    let summaries = summarize(&rows);
    let baseline = args.baseline.clone().unwrap_or(default_baseline);
    if !summaries.iter().any(|summary| summary.target == baseline) {
        return Err(format!("baseline target `{baseline}` was not present in CSV").into());
    }
    let comparisons = compare_to_baseline(&rows, &baseline);

    let markdown = render_markdown(&args, &summaries, &comparisons, &baseline);
    let json = render_json(&args, &summaries, &comparisons, &baseline);

    if let Some(path) = args.markdown {
        std::fs::write(path, markdown)?;
    } else {
        print!("{markdown}");
    }
    if let Some(path) = args.json {
        std::fs::write(path, json)?;
    }
    Ok(())
}

fn read_rows(path: &Path) -> Result<Vec<CsvRow>, BoxError> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let Some(header) = lines.next() else {
        return Ok(Vec::new());
    };
    let columns = header.split(',').collect::<Vec<_>>();
    let index = |name: &str| -> Result<usize, BoxError> {
        columns
            .iter()
            .position(|column| *column == name)
            .ok_or_else(|| format!("CSV header missing `{name}`").into())
    };

    let target_i = index("target")?;
    let family_i = index("family")?;
    let command_i = index("command")?;
    let case_i = index("case")?;
    let clients_i = index("clients")?;
    let duration_i = index("duration_s")?;
    let ops_i = index("ops")?;
    let ops_sec_i = index("ops_per_sec")?;
    let avg_us_i = index("avg_us")?;
    let errors_i = index("errors")?;
    let profile_i = index("profile")?;

    let mut rows = Vec::new();
    for (line_number, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != columns.len() {
            return Err(format!(
                "CSV line {} has {} fields, expected {}",
                line_number + 2,
                fields.len(),
                columns.len()
            )
            .into());
        }
        rows.push(CsvRow {
            target: fields[target_i].to_string(),
            family: fields[family_i].to_string(),
            command: fields[command_i].to_string(),
            case_name: fields[case_i].to_string(),
            clients: fields[clients_i].parse()?,
            duration_s: fields[duration_i].parse()?,
            ops: fields[ops_i].parse()?,
            ops_per_sec: fields[ops_sec_i].parse()?,
            avg_us: fields[avg_us_i].parse()?,
            errors: fields[errors_i].parse()?,
            profile: fields[profile_i].to_string(),
        });
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RowKey {
    target: String,
    family: String,
    command: String,
    case_name: String,
    clients: usize,
    profile: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CaseKey {
    family: String,
    command: String,
    case_name: String,
    clients: usize,
    profile: String,
}

impl CsvRow {
    fn row_key(&self) -> RowKey {
        RowKey {
            target: self.target.clone(),
            family: self.family.clone(),
            command: self.command.clone(),
            case_name: self.case_name.clone(),
            clients: self.clients,
            profile: self.profile.clone(),
        }
    }

    fn case_key(&self) -> CaseKey {
        CaseKey {
            family: self.family.clone(),
            command: self.command.clone(),
            case_name: self.case_name.clone(),
            clients: self.clients,
            profile: self.profile.clone(),
        }
    }
}

fn merge_rows(primary_rows: Vec<CsvRow>, reference_rows: Vec<CsvRow>) -> Vec<CsvRow> {
    let mut by_key = BTreeMap::<RowKey, CsvRow>::new();
    for row in reference_rows {
        by_key.insert(row.row_key(), row);
    }
    for row in primary_rows {
        by_key.insert(row.row_key(), row);
    }
    by_key.into_values().collect()
}

fn summarize(rows: &[CsvRow]) -> Vec<TargetSummary> {
    let mut by_target = BTreeMap::<String, TargetSummary>::new();
    for row in rows {
        let summary = by_target
            .entry(row.target.clone())
            .or_insert_with(|| TargetSummary {
                target: row.target.clone(),
                clients: row.clients,
                duration_s: row.duration_s,
                ..TargetSummary::default()
            });
        summary.cases += 1;
        summary.total_ops = summary.total_ops.saturating_add(row.ops);
        summary.sum_ops_per_sec += row.ops_per_sec;
        summary.mean_avg_us += row.avg_us;
        summary.errors = summary.errors.saturating_add(row.errors);
        if row.profile == "large" {
            summary.large_cases += 1;
        } else {
            summary.small_cases += 1;
        }
        summary.rows.push(row.clone());
    }

    by_target
        .into_values()
        .map(|mut summary| {
            if summary.cases > 0 {
                summary.mean_avg_us /= summary.cases as f64;
            }
            summary.rows.sort_by(|left, right| {
                right
                    .avg_us
                    .partial_cmp(&left.avg_us)
                    .unwrap_or(Ordering::Equal)
            });
            summary
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
struct TargetComparison {
    target: String,
    common_cases: usize,
    target_sum_ops_per_sec: f64,
    baseline_sum_ops_per_sec: f64,
    target_mean_avg_us: f64,
    baseline_mean_avg_us: f64,
    target_errors: u64,
    baseline_errors: u64,
}

fn compare_to_baseline(rows: &[CsvRow], baseline: &str) -> Vec<TargetComparison> {
    let mut baseline_by_case = BTreeMap::<CaseKey, &CsvRow>::new();
    let mut rows_by_target = BTreeMap::<String, BTreeMap<CaseKey, &CsvRow>>::new();

    for row in rows {
        if row.target == baseline {
            baseline_by_case.insert(row.case_key(), row);
        } else {
            rows_by_target
                .entry(row.target.clone())
                .or_default()
                .insert(row.case_key(), row);
        }
    }

    rows_by_target
        .into_iter()
        .filter_map(|(target, target_rows)| {
            let mut comparison = TargetComparison {
                target,
                ..TargetComparison::default()
            };
            for (case, target_row) in target_rows {
                let Some(baseline_row) = baseline_by_case.get(&case) else {
                    continue;
                };
                comparison.common_cases += 1;
                comparison.target_sum_ops_per_sec += target_row.ops_per_sec;
                comparison.baseline_sum_ops_per_sec += baseline_row.ops_per_sec;
                comparison.target_mean_avg_us += target_row.avg_us;
                comparison.baseline_mean_avg_us += baseline_row.avg_us;
                comparison.target_errors =
                    comparison.target_errors.saturating_add(target_row.errors);
                comparison.baseline_errors = comparison
                    .baseline_errors
                    .saturating_add(baseline_row.errors);
            }

            if comparison.common_cases == 0 {
                return None;
            }
            comparison.target_mean_avg_us /= comparison.common_cases as f64;
            comparison.baseline_mean_avg_us /= comparison.common_cases as f64;
            Some(comparison)
        })
        .collect()
}

fn render_markdown(
    args: &Args,
    summaries: &[TargetSummary],
    comparisons: &[TargetComparison],
    baseline: &str,
) -> String {
    let baseline_ops = summaries
        .iter()
        .find(|summary| summary.target == baseline)
        .map(|summary| summary.sum_ops_per_sec)
        .unwrap_or(0.0);

    let mut out = String::new();
    writeln!(out, "# {}", args.label).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Primary CSV: `{}`", args.csv.display()).unwrap();
    if args.reference_csvs.is_empty() {
        writeln!(out, "Reference CSVs: none").unwrap();
    } else {
        writeln!(out, "Reference CSVs:").unwrap();
        for path in &args.reference_csvs {
            writeln!(out, "- `{}`", path.display()).unwrap();
        }
    }
    writeln!(out).unwrap();
    writeln!(out, "## Target Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "| Target | Cases | Clients | Duration | Sum Ops/sec | Mean Avg us | Errors | vs `{}` |",
        baseline
    )
    .unwrap();
    writeln!(
        out,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .unwrap();
    for summary in summaries {
        let ratio = ratio(summary.sum_ops_per_sec, baseline_ops);
        writeln!(
            out,
            "| {} | {} | {} | {} | {:.1} | {:.1} | {} | {} |",
            summary.target,
            summary.cases,
            summary.clients,
            summary.duration_s,
            summary.sum_ops_per_sec,
            summary.mean_avg_us,
            summary.errors,
            ratio,
        )
        .unwrap();
    }

    writeln!(out).unwrap();
    writeln!(out, "## Common Cases vs Baseline").unwrap();
    writeln!(out).unwrap();
    if comparisons.is_empty() {
        writeln!(
            out,
            "_No comparison targets shared command cases with `{}`._",
            baseline
        )
        .unwrap();
    } else {
        writeln!(
            out,
            "| Target | Common Cases | Target Sum Ops/sec | `{}` Sum Ops/sec | Ratio | Target Mean Avg us | `{}` Mean Avg us | Target Errors | `{}` Errors |",
            baseline, baseline, baseline
        )
        .unwrap();
        writeln!(
            out,
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
        )
        .unwrap();
        for comparison in comparisons {
            writeln!(
                out,
                "| {} | {} | {:.1} | {:.1} | {} | {:.1} | {:.1} | {} | {} |",
                comparison.target,
                comparison.common_cases,
                comparison.target_sum_ops_per_sec,
                comparison.baseline_sum_ops_per_sec,
                ratio(
                    comparison.target_sum_ops_per_sec,
                    comparison.baseline_sum_ops_per_sec
                ),
                comparison.target_mean_avg_us,
                comparison.baseline_mean_avg_us,
                comparison.target_errors,
                comparison.baseline_errors,
            )
            .unwrap();
        }
    }

    for summary in summaries {
        writeln!(out).unwrap();
        writeln!(out, "## Slowest Cases: {}", summary.target).unwrap();
        writeln!(out).unwrap();
        writeln!(
            out,
            "| Family | Command | Case | Profile | Ops/sec | Avg us | Errors |"
        )
        .unwrap();
        writeln!(out, "| --- | --- | --- | --- | ---: | ---: | ---: |").unwrap();
        for row in summary.rows.iter().take(args.slowest) {
            writeln!(
                out,
                "| {} | `{}` | {} | {} | {:.1} | {:.1} | {} |",
                row.family,
                row.command,
                markdown_cell(&row.case_name),
                row.profile,
                row.ops_per_sec,
                row.avg_us,
                row.errors,
            )
            .unwrap();
        }
    }
    out
}

fn render_json(
    args: &Args,
    summaries: &[TargetSummary],
    comparisons: &[TargetComparison],
    baseline: &str,
) -> String {
    let baseline_ops = summaries
        .iter()
        .find(|summary| summary.target == baseline)
        .map(|summary| summary.sum_ops_per_sec)
        .unwrap_or(0.0);

    let mut out = String::new();
    writeln!(out, "{{").unwrap();
    writeln!(out, "  \"schema_version\": 2,").unwrap();
    writeln!(out, "  \"label\": \"{}\",", json_escape(&args.label)).unwrap();
    writeln!(
        out,
        "  \"source_csv\": \"{}\",",
        json_escape(&args.csv.display().to_string())
    )
    .unwrap();
    writeln!(
        out,
        "  \"primary_csv\": \"{}\",",
        json_escape(&args.csv.display().to_string())
    )
    .unwrap();
    writeln!(out, "  \"reference_csvs\": [").unwrap();
    for (index, path) in args.reference_csvs.iter().enumerate() {
        let comma = if index + 1 == args.reference_csvs.len() {
            ""
        } else {
            ","
        };
        writeln!(
            out,
            "    \"{}\"{}",
            json_escape(&path.display().to_string()),
            comma
        )
        .unwrap();
    }
    writeln!(out, "  ],").unwrap();
    writeln!(out, "  \"baseline\": \"{}\",", json_escape(baseline)).unwrap();
    writeln!(out, "  \"targets\": [").unwrap();
    for (index, summary) in summaries.iter().enumerate() {
        let comma = if index + 1 == summaries.len() {
            ""
        } else {
            ","
        };
        let ratio = match baseline_ops {
            value if value > 0.0 => summary.sum_ops_per_sec / value,
            _ => 0.0,
        };
        writeln!(out, "    {{").unwrap();
        writeln!(
            out,
            "      \"target\": \"{}\",",
            json_escape(&summary.target)
        )
        .unwrap();
        writeln!(out, "      \"cases\": {},", summary.cases).unwrap();
        writeln!(out, "      \"small_cases\": {},", summary.small_cases).unwrap();
        writeln!(out, "      \"large_cases\": {},", summary.large_cases).unwrap();
        writeln!(out, "      \"clients\": {},", summary.clients).unwrap();
        writeln!(out, "      \"duration_s\": {},", summary.duration_s).unwrap();
        writeln!(out, "      \"total_ops\": {},", summary.total_ops).unwrap();
        writeln!(
            out,
            "      \"sum_ops_per_sec\": {:.3},",
            summary.sum_ops_per_sec
        )
        .unwrap();
        writeln!(out, "      \"mean_avg_us\": {:.3},", summary.mean_avg_us).unwrap();
        writeln!(out, "      \"errors\": {},", summary.errors).unwrap();
        writeln!(out, "      \"baseline_ratio\": {:.6}", ratio).unwrap();
        writeln!(out, "    }}{comma}").unwrap();
    }
    writeln!(out, "  ],").unwrap();
    writeln!(out, "  \"comparisons\": [").unwrap();
    for (index, comparison) in comparisons.iter().enumerate() {
        let comma = if index + 1 == comparisons.len() {
            ""
        } else {
            ","
        };
        let ratio = match comparison.baseline_sum_ops_per_sec {
            value if value > 0.0 => comparison.target_sum_ops_per_sec / value,
            _ => 0.0,
        };
        writeln!(out, "    {{").unwrap();
        writeln!(
            out,
            "      \"target\": \"{}\",",
            json_escape(&comparison.target)
        )
        .unwrap();
        writeln!(out, "      \"baseline\": \"{}\",", json_escape(baseline)).unwrap();
        writeln!(out, "      \"common_cases\": {},", comparison.common_cases).unwrap();
        writeln!(
            out,
            "      \"target_sum_ops_per_sec\": {:.3},",
            comparison.target_sum_ops_per_sec
        )
        .unwrap();
        writeln!(
            out,
            "      \"baseline_sum_ops_per_sec\": {:.3},",
            comparison.baseline_sum_ops_per_sec
        )
        .unwrap();
        writeln!(
            out,
            "      \"target_mean_avg_us\": {:.3},",
            comparison.target_mean_avg_us
        )
        .unwrap();
        writeln!(
            out,
            "      \"baseline_mean_avg_us\": {:.3},",
            comparison.baseline_mean_avg_us
        )
        .unwrap();
        writeln!(
            out,
            "      \"target_errors\": {},",
            comparison.target_errors
        )
        .unwrap();
        writeln!(
            out,
            "      \"baseline_errors\": {},",
            comparison.baseline_errors
        )
        .unwrap();
        writeln!(out, "      \"baseline_ratio\": {:.6}", ratio).unwrap();
        writeln!(out, "    }}{comma}").unwrap();
    }
    writeln!(out, "  ]").unwrap();
    writeln!(out, "}}").unwrap();
    out
}

fn ratio(value: f64, baseline: f64) -> String {
    if baseline <= 0.0 {
        return "n/a".to_string();
    }
    format!("{:.2}x", value / baseline)
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|")
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
    use super::{CsvRow, compare_to_baseline, merge_rows, summarize};

    #[test]
    fn summary_rolls_up_targets() {
        let rows = vec![
            row("fast-cache", "GET", 100.0, 10.0, 0),
            row("fast-cache", "SET", 200.0, 20.0, 1),
            row("redis", "GET", 50.0, 30.0, 0),
        ];
        let summaries = summarize(&rows);
        let fast_cache = summaries
            .iter()
            .find(|summary| summary.target == "fast-cache")
            .unwrap();

        assert_eq!(fast_cache.cases, 2);
        assert_eq!(fast_cache.errors, 1);
        assert_eq!(fast_cache.sum_ops_per_sec, 300.0);
        assert_eq!(fast_cache.mean_avg_us, 15.0);
    }

    #[test]
    fn merge_prefers_primary_rows_for_same_target_case() {
        let reference_rows = vec![row("fast-cache", "GET", 100.0, 10.0, 0)];
        let primary_rows = vec![row("fast-cache", "GET", 250.0, 5.0, 0)];

        let merged = merge_rows(primary_rows, reference_rows);

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].ops_per_sec, 250.0);
    }

    #[test]
    fn comparisons_use_only_common_cases() {
        let rows = vec![
            row("fast-cache", "GET", 100.0, 10.0, 0),
            row("fast-cache", "SET", 300.0, 20.0, 0),
            row("redis", "GET", 50.0, 40.0, 1),
            row("redis", "MGET", 25.0, 80.0, 0),
        ];

        let comparisons = compare_to_baseline(&rows, "fast-cache");

        assert_eq!(comparisons.len(), 1);
        assert_eq!(comparisons[0].target, "redis");
        assert_eq!(comparisons[0].common_cases, 1);
        assert_eq!(comparisons[0].baseline_sum_ops_per_sec, 100.0);
        assert_eq!(comparisons[0].target_sum_ops_per_sec, 50.0);
        assert_eq!(comparisons[0].target_errors, 1);
    }

    fn row(target: &str, command: &str, ops_per_sec: f64, avg_us: f64, errors: u64) -> CsvRow {
        CsvRow {
            target: target.to_string(),
            family: "string".to_string(),
            command: command.to_string(),
            case_name: command.to_string(),
            clients: 1,
            duration_s: 1,
            ops: ops_per_sec as u64,
            ops_per_sec,
            avg_us,
            errors,
            profile: "small".to_string(),
        }
    }
}
