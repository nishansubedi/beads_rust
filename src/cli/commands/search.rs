//! Search command implementation.
//!
//! Classic bd-style LIKE search across title/description/id with list-like filters.

use crate::cli::{ListArgs, OutputFormat, SearchArgs, resolve_output_format_with_outer_mode};
use crate::config;
use crate::error::{BeadsError, Result};
use crate::format::{
    IssueWithCounts, TextFormatOptions, csv, format_issue_line_with, terminal_width,
};
use crate::model::{IssueType, Priority, Status};
use crate::output::{IssueTable, IssueTableColumns, OutputContext, OutputMode};
use crate::storage::{ListFilters, SqliteStorage};
use chrono::Utc;
use regex::{Regex, RegexBuilder};
use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::str::FromStr;

/// Execute the search command.
///
/// # Errors
///
/// Returns an error if the query is empty, the database cannot be opened,
/// or the query fails.
#[allow(clippy::too_many_lines)]
pub fn execute(
    args: &SearchArgs,
    _json: bool,
    cli: &config::CliOverrides,
    outer_ctx: &OutputContext,
) -> Result<()> {
    let query = args.query.trim();
    if query.is_empty() {
        return Err(BeadsError::Validation {
            field: "query".to_string(),
            reason: "search query cannot be empty".to_string(),
        });
    }

    let beads_dir = config::discover_beads_dir_with_cli(cli)?;
    let storage_ctx = config::open_storage_with_cli(&beads_dir, cli)?;
    let storage = &storage_ctx.storage;

    let mut filters = build_filters(&args.filters)?;
    let client_filters = needs_client_filters(&args.filters);
    let needs_post_query_ordering = requires_post_query_ordering(&args.filters, client_filters);
    let limit = if needs_post_query_ordering {
        filters.limit.take()
    } else {
        None
    };
    if needs_post_query_ordering {
        filters.sort = None;
        filters.reverse = false;
    }

    let issues = storage.search_issues(query, &filters)?;
    let mut issues = if client_filters {
        apply_client_filters(issues, &args.filters)?
    } else {
        issues
    };

    if needs_post_query_ordering {
        apply_issue_sort(&mut issues, args.filters.sort.as_deref())?;
        if args.filters.reverse {
            issues.reverse();
        }
        if let Some(limit) = limit
            && limit > 0
            && issues.len() > limit
        {
            issues.truncate(limit);
        }
    }

    let output_format = resolve_output_format_with_outer_mode(
        args.filters.format,
        outer_ctx.inherited_output_mode(),
        false,
    );

    let quiet = cli.quiet.unwrap_or(false);
    let early_ctx = OutputContext::from_output_format(output_format, quiet, true);
    if matches!(early_ctx.mode(), OutputMode::Quiet) {
        return Ok(());
    }

    let config_layer = storage_ctx.load_config(cli)?;
    let use_color = config::should_use_color(&config_layer);
    let max_width = if std::io::stdout().is_terminal() {
        Some(terminal_width())
    } else {
        None
    };
    let format_options = TextFormatOptions {
        use_color,
        max_width,
        wrap: args.filters.wrap,
    };
    let ctx = OutputContext::from_output_format(output_format, quiet, !use_color);

    match output_format {
        OutputFormat::Json => {
            let issues_with_counts = attach_counts(storage, issues)?;
            ctx.json_pretty(&issues_with_counts);
            return Ok(());
        }
        OutputFormat::Toon => {
            let issues_with_counts = attach_counts(storage, issues)?;
            ctx.toon_with_stats(&issues_with_counts, args.filters.stats);
            return Ok(());
        }
        OutputFormat::Csv => {
            let fields = csv::parse_fields(args.filters.fields.as_deref());
            let csv_output = csv::format_csv(&issues, &fields);
            print!("{csv_output}");
            return Ok(());
        }
        OutputFormat::Text => {}
    }

    if matches!(ctx.mode(), OutputMode::Rich) {
        let context_snippets = build_context_snippets(&issues, query);
        let show_context = !context_snippets.is_empty();
        let columns = IssueTableColumns {
            id: true,
            priority: true,
            status: true,
            issue_type: true,
            title: true,
            assignee: true,
            context: show_context,
            ..Default::default()
        };
        let mut table = IssueTable::new(&issues, ctx.theme())
            .columns(columns)
            .title(format!(
                "Search: \"{}\" - {} result{}",
                query,
                issues.len(),
                if issues.len() == 1 { "" } else { "s" }
            ))
            .highlight_query(query)
            .wrap(args.filters.wrap);
        if args.filters.wrap {
            table = table.width(Some(ctx.width()));
        }
        if show_context {
            table = table.context_snippets(context_snippets);
        }
        ctx.render(&table.build());
        return Ok(());
    }

    ctx.info(&format!(
        "Found {} issue(s) matching '{}'",
        issues.len(),
        query
    ));
    for issue in &issues {
        let line = format_issue_line_with(issue, format_options);
        ctx.print_line(&line);
    }

    Ok(())
}

fn attach_counts(
    storage: &SqliteStorage,
    issues: Vec<crate::model::Issue>,
) -> Result<Vec<IssueWithCounts>> {
    if issues.is_empty() {
        return Ok(Vec::new());
    }

    let issue_ids: Vec<String> = issues.iter().map(|issue| issue.id.clone()).collect();
    let labels_map = storage.get_labels_for_issues(&issue_ids)?;
    let (dep_counts, dependent_counts) = storage.count_relation_counts_for_issues(&issue_ids)?;

    Ok(issues
        .into_iter()
        .map(|mut issue| {
            let dependency_count = *dep_counts.get(&issue.id).unwrap_or(&0);
            let dependent_count = *dependent_counts.get(&issue.id).unwrap_or(&0);
            issue.labels = labels_map.get(&issue.id).cloned().unwrap_or_default();
            IssueWithCounts {
                issue,
                dependency_count,
                dependent_count,
            }
        })
        .collect())
}

fn build_context_snippets(issues: &[crate::model::Issue], query: &str) -> HashMap<String, String> {
    let Some(regex) = build_highlight_regex(query) else {
        return HashMap::new();
    };

    let mut snippets = HashMap::new();
    for issue in issues {
        if let Some(description) = issue.description.as_deref()
            && let Some(mat) = regex.find(description)
        {
            let snippet = snippet_around_match(description, mat.start(), mat.end(), 32);
            if !snippet.is_empty() {
                snippets.insert(issue.id.clone(), snippet);
                continue;
            }
        }

        if regex.is_match(&issue.id) && !regex.is_match(&issue.title) {
            snippets.insert(issue.id.clone(), "ID match".to_string());
        }
    }

    snippets
}

fn build_highlight_regex(query: &str) -> Option<Regex> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return None;
    }
    let pattern = regex::escape(trimmed);
    RegexBuilder::new(&pattern)
        .case_insensitive(true)
        .build()
        .ok()
}

fn snippet_around_match(text: &str, start: usize, end: usize, radius: usize) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut char_starts: Vec<usize> = text.char_indices().map(|(i, _)| i).collect();
    char_starts.push(text.len());

    let total_chars = char_starts.len().saturating_sub(1);
    let start_char = char_starts.partition_point(|&idx| idx < start);
    let end_char = char_starts.partition_point(|&idx| idx < end);

    let snippet_start_char = start_char.saturating_sub(radius);
    let snippet_end_char = (end_char + radius).min(total_chars);

    let snippet_start_byte = char_starts[snippet_start_char];
    let snippet_end_byte = char_starts[snippet_end_char];

    let mut snippet = text[snippet_start_byte..snippet_end_byte]
        .trim()
        .to_string();
    snippet = normalize_whitespace(&snippet);

    if snippet_start_char > 0 {
        snippet.insert_str(0, "...");
    }
    if snippet_end_char < total_chars {
        snippet.push_str("...");
    }

    snippet
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn build_filters(args: &ListArgs) -> Result<ListFilters> {
    let statuses = if args.status.is_empty() {
        None
    } else {
        Some(
            args.status
                .iter()
                .map(|s| s.parse())
                .collect::<Result<Vec<Status>>>()?,
        )
    };

    let types = if args.type_.is_empty() {
        None
    } else {
        Some(
            args.type_
                .iter()
                .map(|t| t.parse())
                .collect::<Result<Vec<IssueType>>>()?,
        )
    };

    let priorities = if args.priority.is_empty() {
        None
    } else {
        let mut parsed = Vec::new();
        for p in &args.priority {
            parsed.push(Priority::from_str(p)?);
        }
        Some(parsed)
    };

    let include_closed = args.all
        || statuses
            .as_ref()
            .is_some_and(|parsed| parsed.iter().any(Status::is_terminal));

    // Deferred issues are included by default (consistent with "open" status semantics).
    let include_deferred = args.deferred
        || args.all
        || statuses.is_none()
        || statuses
            .as_ref()
            .is_some_and(|parsed| parsed.contains(&Status::Deferred));

    Ok(ListFilters {
        statuses,
        types,
        priorities,
        assignee: args.assignee.clone(),
        unassigned: args.unassigned,
        include_closed,
        include_deferred,
        include_templates: false,
        title_contains: args.title_contains.clone(),
        limit: args.limit,
        sort: args.sort.clone(),
        reverse: args.reverse,
        labels: if args.label.is_empty() {
            None
        } else {
            Some(args.label.clone())
        },
        labels_or: if args.label_any.is_empty() {
            None
        } else {
            Some(args.label_any.clone())
        },
        updated_before: None,
        updated_after: None,
    })
}

fn needs_client_filters(args: &ListArgs) -> bool {
    !args.id.is_empty()
        || args.priority_min.is_some()
        || args.priority_max.is_some()
        || args.desc_contains.is_some()
        || args.notes_contains.is_some()
        || args.deferred
        || args.overdue
}

fn requires_post_query_ordering(args: &ListArgs, client_filters: bool) -> bool {
    client_filters || args.sort.is_some()
}

fn apply_client_filters(
    issues: Vec<crate::model::Issue>,
    args: &ListArgs,
) -> Result<Vec<crate::model::Issue>> {
    let id_filter: Option<HashSet<&str>> = if args.id.is_empty() {
        None
    } else {
        Some(args.id.iter().map(String::as_str).collect())
    };

    let mut filtered = Vec::new();
    let now = Utc::now();
    let min_priority = args.priority_min.map(i32::from);
    let max_priority = args.priority_max.map(i32::from);

    // Use Regex for efficient case-insensitive search without full description allocations
    let desc_regex = args.desc_contains.as_deref().and_then(|needle| {
        RegexBuilder::new(&regex::escape(needle))
            .case_insensitive(true)
            .build()
            .ok()
    });
    let notes_regex = args.notes_contains.as_deref().and_then(|needle| {
        RegexBuilder::new(&regex::escape(needle))
            .case_insensitive(true)
            .build()
            .ok()
    });

    // Deferred issues are included by default when no status filter is specified
    let include_deferred = args.deferred
        || args.status.is_empty()
        || args
            .status
            .iter()
            .any(|status| status.eq_ignore_ascii_case("deferred"));

    if let Some(min) = min_priority
        && !(0..=4).contains(&min)
    {
        return Err(BeadsError::InvalidPriority { priority: min });
    }
    if let Some(max) = max_priority
        && !(0..=4).contains(&max)
    {
        return Err(BeadsError::InvalidPriority { priority: max });
    }

    for issue in issues {
        if let Some(ids) = &id_filter
            && !ids.contains(issue.id.as_str())
        {
            continue;
        }

        if let Some(min) = min_priority
            && issue.priority.0 < min
        {
            continue;
        }
        if let Some(max) = max_priority
            && issue.priority.0 > max
        {
            continue;
        }

        if let Some(ref re) = desc_regex {
            let haystack = issue.description.as_deref().unwrap_or("");
            if !re.is_match(haystack) {
                continue;
            }
        }

        if let Some(ref re) = notes_regex {
            let haystack = issue.notes.as_deref().unwrap_or("");
            if !re.is_match(haystack) {
                continue;
            }
        }

        if !include_deferred && matches!(issue.status, Status::Deferred) {
            continue;
        }

        if args.overdue {
            let overdue = issue.due_at.is_some_and(|due| due < now) && !issue.status.is_terminal();
            if !overdue {
                continue;
            }
        }

        filtered.push(issue);
    }

    Ok(filtered)
}

fn apply_sort_by_issue<T>(
    items: &mut [T],
    sort: Option<&str>,
    mut issue_of: impl FnMut(&T) -> &crate::model::Issue,
) -> Result<()> {
    let Some(sort_key) = sort else {
        return Ok(());
    };

    match sort_key {
        "priority" => items.sort_by_key(|item| issue_of(item).priority),
        "created_at" | "created" => {
            items.sort_by_key(|item| std::cmp::Reverse(issue_of(item).created_at));
        }
        "updated_at" | "updated" => {
            items.sort_by_key(|item| std::cmp::Reverse(issue_of(item).updated_at));
        }
        "title" => items.sort_by_cached_key(|item| issue_of(item).title.to_lowercase()),
        _ => {
            return Err(BeadsError::Validation {
                field: "sort".to_string(),
                reason: format!("invalid sort field '{sort_key}'"),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
fn apply_sort(issues: &mut [IssueWithCounts], sort: Option<&str>) -> Result<()> {
    apply_sort_by_issue(issues, sort, |issue| &issue.issue)
}

fn apply_issue_sort(issues: &mut [crate::model::Issue], sort: Option<&str>) -> Result<()> {
    apply_sort_by_issue(issues, sort, |issue| issue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Issue, IssueType, Priority, Status};
    use chrono::{DateTime, TimeZone, Utc};

    fn make_issue(
        id: &str,
        title: &str,
        description: Option<&str>,
        created_at: DateTime<Utc>,
    ) -> Issue {
        Issue {
            id: id.to_string(),
            content_hash: None,
            title: title.to_string(),
            description: description.map(str::to_string),
            design: None,
            acceptance_criteria: None,
            notes: None,
            status: Status::Open,
            priority: Priority::MEDIUM,
            issue_type: IssueType::Task,
            assignee: None,
            owner: None,
            estimated_minutes: None,
            created_at,
            created_by: None,
            updated_at: created_at,
            closed_at: None,
            close_reason: None,
            closed_by_session: None,
            due_at: None,
            defer_until: None,
            external_ref: None,
            source_system: None,
            source_repo: None,
            deleted_at: None,
            deleted_by: None,
            delete_reason: None,
            original_type: None,
            compaction_level: None,
            compacted_at: None,
            compacted_at_commit: None,
            original_size: None,
            sender: None,
            ephemeral: false,
            pinned: false,
            is_template: false,
            labels: vec![],
            dependencies: vec![],
            comments: vec![],
        }
    }

    #[test]
    fn test_search_matches_title_description_id() {
        let mut storage = SqliteStorage::open_memory().expect("db");
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

        let issue1 = make_issue("bd-001", "Alpha title", None, t1);
        let issue2 = make_issue("bd-002", "Other", Some("alpha desc"), t2);
        let issue3 = make_issue("bd-xyz", "Other", None, t3);

        storage.create_issue(&issue1, "tester").expect("create");
        storage.create_issue(&issue2, "tester").expect("create");
        storage.create_issue(&issue3, "tester").expect("create");

        let filters = ListFilters::default();
        let results = storage.search_issues("alpha", &filters).expect("search");
        let ids: Vec<String> = results.into_iter().map(|issue| issue.id).collect();
        assert!(ids.contains(&"bd-001".to_string()));
        assert!(ids.contains(&"bd-002".to_string()));
        assert!(!ids.contains(&"bd-xyz".to_string()));

        let results = storage.search_issues("xyz", &filters).expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "bd-xyz");
    }

    #[test]
    fn test_label_filters_do_not_require_client_filtering() {
        let args = ListArgs {
            label: vec!["backend".to_string()],
            ..ListArgs::default()
        };

        assert!(!needs_client_filters(&args));
    }

    #[test]
    fn test_label_any_filters_do_not_require_client_filtering() {
        let args = ListArgs {
            label_any: vec!["backend".to_string(), "ops".to_string()],
            ..ListArgs::default()
        };

        assert!(!needs_client_filters(&args));
    }

    #[test]
    fn test_requires_post_query_ordering_only_for_client_filters_or_explicit_sort() {
        let args = ListArgs::default();
        assert!(!requires_post_query_ordering(&args, false));

        let args = ListArgs {
            reverse: true,
            ..ListArgs::default()
        };
        assert!(!requires_post_query_ordering(&args, false));

        let args = ListArgs {
            sort: Some("updated".to_string()),
            ..ListArgs::default()
        };
        assert!(requires_post_query_ordering(&args, false));

        let args = ListArgs {
            desc_contains: Some("needle".to_string()),
            ..ListArgs::default()
        };
        assert!(requires_post_query_ordering(&args, true));
    }

    #[test]
    fn test_search_respects_sort_before_limit() {
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();

        let mut newer_created_but_older_updated = make_issue("bd-a", "Alpha", None, t3);
        newer_created_but_older_updated.updated_at = t1;

        let mut older_created_but_newer_updated = make_issue("bd-b", "Beta", None, t1);
        older_created_but_newer_updated.updated_at = t3;

        let mut items = vec![
            newer_created_but_older_updated,
            older_created_but_newer_updated,
        ];

        apply_issue_sort(&mut items, Some("updated")).expect("sort");
        items.truncate(1);
        assert_eq!(items[0].id, "bd-b");
    }

    #[test]
    fn test_search_respects_reverse_without_explicit_sort() {
        let mut storage = SqliteStorage::open_memory().expect("db");
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();

        let older_high_priority = make_issue("bd-old", "match old", None, t1);
        let newer_low_priority = make_issue("bd-new", "match new", None, t2);

        storage
            .create_issue(&older_high_priority, "tester")
            .expect("create");
        storage
            .create_issue(&newer_low_priority, "tester")
            .expect("create");

        let forward = storage
            .search_issues("match", &ListFilters::default())
            .expect("search");
        let reverse = storage
            .search_issues(
                "match",
                &ListFilters {
                    reverse: true,
                    ..Default::default()
                },
            )
            .expect("search");

        assert_eq!(forward[0].id, "bd-new");
        assert_eq!(forward[1].id, "bd-old");
        assert_eq!(reverse[0].id, "bd-old");
        assert_eq!(reverse[1].id, "bd-new");
    }

    #[test]
    fn test_search_command_pipeline_applies_reverse_once() {
        let mut storage = SqliteStorage::open_memory().expect("db");
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();

        let older = make_issue("bd-old", "match old", None, t1);
        let newer = make_issue("bd-new", "match new", None, t2);
        storage.create_issue(&older, "tester").expect("create");
        storage.create_issue(&newer, "tester").expect("create");

        let args = ListArgs {
            reverse: true,
            ..Default::default()
        };
        let mut filters = build_filters(&args).expect("filters");
        let limit = filters.limit.take();
        filters.sort = None;
        filters.reverse = false;

        let mut issues = storage.search_issues("match", &filters).expect("search");

        apply_issue_sort(&mut issues, args.sort.as_deref()).expect("sort");
        if args.reverse {
            issues.reverse();
        }
        if let Some(limit) = limit {
            issues.truncate(limit);
        }

        assert_eq!(issues[0].id, "bd-old");
        assert_eq!(issues[1].id, "bd-new");
    }

    #[test]
    fn test_sort_by_title_and_reverse() {
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();

        let issue_a = make_issue("bd-a", "Alpha", None, t1);
        let issue_b = make_issue("bd-b", "Beta", None, t2);

        let mut items = vec![
            IssueWithCounts {
                issue: issue_b,
                dependency_count: 0,
                dependent_count: 0,
            },
            IssueWithCounts {
                issue: issue_a,
                dependency_count: 0,
                dependent_count: 0,
            },
        ];

        apply_sort(&mut items, Some("title")).expect("sort");
        assert_eq!(items[0].issue.title, "Alpha");
        items.reverse();
        assert_eq!(items[0].issue.title, "Beta");
    }

    #[test]
    fn test_sort_created_at_desc_default() {
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();

        let issue_old = make_issue("bd-old", "Old", None, t1);
        let issue_new = make_issue("bd-new", "New", None, t2);

        let mut items = vec![
            IssueWithCounts {
                issue: issue_old,
                dependency_count: 0,
                dependent_count: 0,
            },
            IssueWithCounts {
                issue: issue_new,
                dependency_count: 0,
                dependent_count: 0,
            },
        ];

        apply_sort(&mut items, Some("created_at")).expect("sort");
        assert_eq!(items[0].issue.id, "bd-new");
    }

    #[test]
    fn test_attach_counts_backfills_labels() {
        let mut storage = SqliteStorage::open_memory().expect("db");
        let issue = Issue {
            id: "bd-labeled".to_string(),
            title: "Labeled issue".to_string(),
            ..Issue::default()
        };
        storage.create_issue(&issue, "tester").expect("create");
        storage
            .add_label("bd-labeled", "backend", "tester")
            .expect("label");

        let stored_issue = storage
            .get_issue("bd-labeled")
            .expect("get")
            .expect("issue exists");
        assert!(
            stored_issue.labels.is_empty(),
            "labels are loaded separately"
        );

        let hydrated = attach_counts(&storage, vec![stored_issue]).expect("attach counts");
        assert_eq!(hydrated.len(), 1);
        assert_eq!(hydrated[0].issue.labels, vec!["backend".to_string()]);
    }
}
