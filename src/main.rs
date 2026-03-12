use beads_rust::cli::commands;
use beads_rust::cli::{Cli, Commands, OutputFormat};
use beads_rust::config;
use beads_rust::logging::init_logging;
use beads_rust::output::OutputContext;
use beads_rust::sync::{auto_flush, auto_import_if_stale};
use beads_rust::{BeadsError, Result, StructuredError};
use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use tracing::debug;

#[allow(clippy::too_many_lines)]
fn main() {
    CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();
    let json_error_mode = should_render_errors_as_json(&cli);
    let output_ctx = OutputContext::from_args(&cli);
    let is_mutating = is_mutating_command(&cli.command);
    let needs_bootstrap_context = should_auto_import(&cli.command);

    // Initialize logging
    if let Err(e) = init_logging(cli.verbose, cli.quiet, None) {
        eprintln!("Failed to initialize logging: {e}");
    }

    let overrides = build_cli_overrides(&cli);

    // Phase 1: Startup & Discovery (One-time)
    let ctx = match StartupContext::init(&overrides) {
        Ok(ctx) => ctx,
        Err(e) => {
            if needs_bootstrap_context {
                handle_error(&e, json_error_mode);
            }
            StartupContext::empty(overrides.clone())
        }
    };

    // Phase 2: Open Storage (One-time)
    let storage_enabled = ctx.is_initialized() && !ctx.no_db();
    let should_auto_flush_now = is_mutating && !ctx.no_auto_flush();
    let should_preopen_storage = should_preopen_storage(
        storage_enabled,
        needs_bootstrap_context,
        should_auto_flush_now,
    );
    let mut storage_result = if should_preopen_storage {
        match open_storage_from_ctx(&ctx) {
            Ok(res) => Some(res),
            Err(e) => {
                if needs_bootstrap_context {
                    handle_error(&e, json_error_mode);
                }
                None
            }
        }
    } else {
        None
    };

    // Phase 3: Auto-Import
    if let (Some(res), Some(paths)) = (storage_result.as_mut(), ctx.paths.as_ref())
        && should_auto_import(&cli.command)
    {
        let expected_prefix = match res.storage.get_config("issue_prefix") {
            Ok(p) => p,
            Err(e) => {
                handle_error(&e, json_error_mode);
            }
        };
        let outcome = auto_import_if_stale(
            &mut res.storage,
            &paths.beads_dir,
            &paths.jsonl_path,
            expected_prefix.as_deref(),
            cli.allow_stale,
            ctx.no_auto_import(),
        );
        if let Err(e) = outcome {
            handle_error(&e, json_error_mode);
        }
    }

    // Phase 4: Command Execution
    let result = match cli.command {
        Commands::Init {
            prefix,
            force,
            backend: _,
        } => commands::init::execute(prefix, force, None, &output_ctx),
        Commands::Create(args) => commands::create::execute(&args, &overrides, &output_ctx),
        Commands::Update(args) => commands::update::execute(&args, &overrides, &output_ctx),
        Commands::Delete(args) => {
            commands::delete::execute(&args, cli.json, &overrides, &output_ctx)
        }
        Commands::List(args) => {
            if let Some(res) = storage_result.as_ref() {
                commands::list::execute_with_storage(&args, &overrides, &output_ctx, res)
            } else {
                commands::list::execute(&args, cli.json, &overrides, &output_ctx)
            }
        }
        Commands::Comments(args) => {
            commands::comments::execute(&args, cli.json, &overrides, &output_ctx)
        }
        Commands::Search(args) => {
            if let Some(res) = storage_result.as_ref() {
                commands::search::execute_with_storage_ctx(&args, &overrides, &output_ctx, res)
            } else {
                commands::search::execute(&args, cli.json, &overrides, &output_ctx)
            }
        }
        Commands::Show(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::show::execute_with_storage_ctx(
                    &args,
                    &overrides,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::show::execute(&args, cli.json, &overrides, &output_ctx)
            }
        }
        Commands::Close(args) => {
            commands::close::execute_cli(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Reopen(args) => {
            commands::reopen::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Q(args) => commands::q::execute(args, &overrides, &output_ctx),
        Commands::Dep { command } => {
            commands::dep::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Epic { command } => {
            commands::epic::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Label { command } => {
            commands::label::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Count(args) => {
            if let Some(res) = storage_result.as_ref() {
                commands::count::execute_with_storage(&args, &output_ctx, &res.storage)
            } else {
                commands::count::execute(&args, cli.json, &overrides, &output_ctx)
            }
        }
        Commands::Stale(args) => storage_result.as_ref().map_or_else(
            || commands::stale::execute(&args, &overrides, &output_ctx),
            |res| commands::stale::execute_with_storage(&args, &output_ctx, &res.storage),
        ),
        Commands::Lint(args) => commands::lint::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Ready(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::ready::execute_with_storage_ctx(
                    &args,
                    &overrides,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::ready::execute(&args, cli.json, &overrides, &output_ctx)
            }
        }
        Commands::Blocked(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::blocked::execute_with_storage_ctx(
                    &args,
                    &overrides,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::blocked::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
            }
        }
        Commands::Sync(args) => commands::sync::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Doctor(args) => commands::doctor::execute(&args, &overrides, &output_ctx),
        Commands::Info(args) => commands::info::execute(&args, &overrides, &output_ctx),
        Commands::Schema(args) => commands::schema::execute(&args, &overrides, &output_ctx),
        Commands::Where => commands::r#where::execute(&overrides, &output_ctx),
        Commands::Version(args) => commands::version::execute(&args, &output_ctx),

        #[cfg(feature = "self_update")]
        Commands::Upgrade(args) => commands::upgrade::execute(&args, &output_ctx),
        Commands::Completions(args) => commands::completions::execute(&args, &output_ctx),
        Commands::Audit { command } => {
            commands::audit::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Stats(args) | Commands::Status(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::stats::execute_with_storage_ctx(
                    &args,
                    &overrides,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::stats::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
            }
        }
        Commands::Config { command } => {
            commands::config::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::History(args) => commands::history::execute(args, &overrides, &output_ctx),
        Commands::Defer(args) => {
            commands::defer::execute_defer(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Undefer(args) => {
            commands::defer::execute_undefer(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Orphans(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::orphans::execute_with_storage_ctx(
                    &args,
                    &overrides,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::orphans::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
            }
        }
        Commands::Changelog(args) => {
            if let (Some(res), Some(beads_dir)) = (storage_result.as_ref(), ctx.beads_dir.as_ref())
            {
                commands::changelog::execute_with_storage_ctx(
                    &args,
                    cli.json || args.robot,
                    &output_ctx,
                    beads_dir,
                    res,
                )
            } else {
                commands::changelog::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
            }
        }
        Commands::Query { command } => commands::query::execute(&command, &overrides, &output_ctx),
        Commands::Graph(args) => storage_result.as_ref().map_or_else(
            || commands::graph::execute(&args, &overrides, &output_ctx),
            |res| commands::graph::execute_with_storage_ctx(&args, &overrides, &output_ctx, res),
        ),
        Commands::Agents(args) => {
            let agents_args = commands::agents::AgentsArgs {
                add: args.add,
                remove: args.remove,
                update: args.update,
                check: args.check,
                dry_run: args.dry_run,
                force: args.force,
            };
            commands::agents::execute(&agents_args, &output_ctx)
        }
    };

    // Handle command result
    if let Err(e) = result {
        handle_error(&e, json_error_mode);
    }

    // Phase 5: Auto-Flush
    if is_mutating
        && !ctx.no_auto_flush()
        && let (Some(res), Some(paths)) = (storage_result.as_mut(), ctx.paths.as_ref())
        && let Err(e) = auto_flush(&mut res.storage, &paths.beads_dir, &paths.jsonl_path)
    {
        debug!(?e, "Auto-flush failed (non-fatal)");
    }
}

struct StartupContext {
    overrides: config::CliOverrides,
    beads_dir: Option<PathBuf>,
    paths: Option<config::ConfigPaths>,
    config: Option<config::ConfigLayer>,
}

impl StartupContext {
    fn init(overrides: &config::CliOverrides) -> Result<Self> {
        let beads_dir = config::discover_beads_dir_with_cli(overrides)?;
        let startup = config::load_startup_config_with_paths(&beads_dir, overrides.db.as_ref())?;

        // Merge startup config with CLI overrides to form the effective bootstrap config
        let mut final_config = startup.merged_config.clone();
        final_config.merge_from(&overrides.as_layer());

        Ok(Self {
            overrides: overrides.clone(),
            beads_dir: Some(beads_dir),
            paths: Some(startup.paths),
            config: Some(final_config),
        })
    }

    fn empty(overrides: config::CliOverrides) -> Self {
        Self {
            overrides,
            beads_dir: None,
            paths: None,
            config: None,
        }
    }

    fn is_initialized(&self) -> bool {
        self.beads_dir.is_some()
    }

    fn no_db(&self) -> bool {
        self.config
            .as_ref()
            .and_then(config::no_db_from_layer)
            .unwrap_or(false)
    }

    fn no_auto_import(&self) -> bool {
        self.config
            .as_ref()
            .and_then(config::no_auto_import_from_layer)
            .unwrap_or(false)
    }

    fn no_auto_flush(&self) -> bool {
        self.config
            .as_ref()
            .and_then(config::no_auto_flush_from_layer)
            .unwrap_or(false)
    }
}

fn open_storage_from_ctx(ctx: &StartupContext) -> Result<config::OpenStorageResult> {
    let beads_dir = ctx.beads_dir.as_ref().ok_or(BeadsError::NotInitialized)?;
    config::open_storage_with_cli(beads_dir, &ctx.overrides)
}

const fn should_preopen_storage(
    storage_enabled: bool,
    needs_bootstrap_context: bool,
    should_auto_flush_now: bool,
) -> bool {
    storage_enabled && (needs_bootstrap_context || should_auto_flush_now)
}

/// Determine if a command potentially mutates data.
const fn is_mutating_command(cmd: &Commands) -> bool {
    match cmd {
        Commands::Create(_)
        | Commands::Update(_)
        | Commands::Delete(_)
        | Commands::Close(_)
        | Commands::Reopen(_)
        | Commands::Q(_)
        | Commands::Defer(_)
        | Commands::Undefer(_) => true,
        Commands::Dep { command } => matches!(
            command,
            beads_rust::cli::DepCommands::Add(_) | beads_rust::cli::DepCommands::Remove(_)
        ),
        Commands::Label { command } => matches!(
            command,
            beads_rust::cli::LabelCommands::Add(_)
                | beads_rust::cli::LabelCommands::Remove(_)
                | beads_rust::cli::LabelCommands::Rename(_)
        ),
        Commands::Comments(args) => matches!(
            args.command.as_ref(),
            Some(beads_rust::cli::CommentCommands::Add(_))
        ),
        Commands::Epic { command } => matches!(
            command,
            beads_rust::cli::EpicCommands::CloseEligible(args) if !args.dry_run
        ),
        _ => false,
    }
}

const fn should_auto_import(cmd: &Commands) -> bool {
    match cmd {
        Commands::List(_)
        | Commands::Show(_)
        | Commands::Search(_)
        | Commands::Ready(_)
        | Commands::Blocked(_)
        | Commands::Count(_)
        | Commands::Stale(_)
        | Commands::Lint(_)
        | Commands::Stats(_)
        | Commands::Status(_)
        | Commands::Changelog(_)
        | Commands::Graph(_)
        | Commands::Create(_)
        | Commands::Update(_)
        | Commands::Delete(_)
        | Commands::Close(_)
        | Commands::Reopen(_)
        | Commands::Q(_)
        | Commands::Defer(_)
        | Commands::Undefer(_)
        | Commands::Comments(_)
        | Commands::Dep { .. }
        | Commands::Label { .. }
        | Commands::Epic { .. }
        | Commands::Query { .. } => true,

        Commands::Init { .. }
        | Commands::Sync(_)
        | Commands::Doctor(_)
        | Commands::Info(_)
        | Commands::Schema(_)
        | Commands::Where
        | Commands::Version(_)
        | Commands::Completions(_)
        | Commands::Audit { .. }
        | Commands::Config { .. }
        | Commands::History(_)
        | Commands::Orphans(_)
        | Commands::Agents(_) => false,

        #[cfg(feature = "self_update")]
        Commands::Upgrade(_) => false,
    }
}

const fn command_requests_robot_json(cmd: &Commands) -> bool {
    match cmd {
        Commands::Close(args) => args.robot,
        Commands::Reopen(args) => args.robot,
        Commands::Ready(args) => args.robot,
        Commands::Blocked(args) => args.robot,
        Commands::Stats(args) | Commands::Status(args) => args.robot,
        Commands::Defer(args) => args.robot,
        Commands::Undefer(args) => args.robot,
        Commands::Orphans(args) => args.robot,
        Commands::Changelog(args) => args.robot,
        Commands::Sync(args) => args.robot,
        _ => false,
    }
}

fn command_requested_output_format(cmd: &Commands) -> Option<OutputFormat> {
    match cmd {
        Commands::List(args) => args.format,
        Commands::Search(args) => args.filters.format,
        Commands::Show(args) => args.format.map(Into::into),
        Commands::Ready(args) => args.format.map(Into::into),
        Commands::Blocked(args) => args.format.map(Into::into),
        Commands::Stats(args) | Commands::Status(args) => args.format.map(Into::into),
        Commands::Schema(args) => args.format.map(Into::into),
        Commands::Dep { command } => match command {
            beads_rust::cli::DepCommands::List(args) => args.format.map(Into::into),
            beads_rust::cli::DepCommands::Tree(_)
            | beads_rust::cli::DepCommands::Add(_)
            | beads_rust::cli::DepCommands::Remove(_)
            | beads_rust::cli::DepCommands::Cycles(_) => None,
        },
        Commands::Query { command } => match command {
            beads_rust::cli::QueryCommands::Run(args) => args.filters.format,
            beads_rust::cli::QueryCommands::Save(_)
            | beads_rust::cli::QueryCommands::List
            | beads_rust::cli::QueryCommands::Delete(_) => None,
        },
        _ => None,
    }
}

fn should_render_errors_as_json_with_env(
    cli: &Cli,
    env_output_format: Option<OutputFormat>,
) -> bool {
    cli.json
        || command_requests_robot_json(&cli.command)
        || matches!(
            command_requested_output_format(&cli.command).or(env_output_format),
            Some(OutputFormat::Json | OutputFormat::Toon)
        )
}

fn should_render_errors_as_json(cli: &Cli) -> bool {
    should_render_errors_as_json_with_env(cli, OutputFormat::from_env())
}

/// Handle errors with structured output support.
fn handle_error(err: &BeadsError, json_mode: bool) -> ! {
    let structured = StructuredError::from_error(err);
    let exit_code = structured.code.exit_code();

    if json_mode {
        let json = structured.to_json();
        eprintln!(
            "{}",
            serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
        );
    } else {
        let use_color = io::stderr().is_terminal();
        eprintln!("{}", structured.to_human(use_color));
    }

    std::process::exit(exit_code);
}

fn build_cli_overrides(cli: &Cli) -> config::CliOverrides {
    config::CliOverrides {
        db: cli.db.clone(),
        actor: cli.actor.clone(),
        identity: None,
        json: Some(cli.json),
        display_color: if cli.no_color { Some(false) } else { None },
        quiet: Some(cli.quiet),
        no_db: if cli.no_db { Some(true) } else { None },
        no_daemon: if cli.no_daemon { Some(true) } else { None },
        no_auto_flush: if cli.no_auto_flush { Some(true) } else { None },
        no_auto_import: if cli.no_auto_import { Some(true) } else { None },
        lock_timeout: cli.lock_timeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn make_create_args() -> beads_rust::cli::CreateArgs {
        beads_rust::cli::CreateArgs {
            title: Some("test-title".to_string()),
            title_flag: None,
            type_: None,
            priority: None,
            description: None,
            assignee: None,
            owner: None,
            labels: Vec::new(),
            parent: None,
            deps: Vec::new(),
            estimate: None,
            due: None,
            defer: None,
            external_ref: None,
            status: None,
            ephemeral: false,
            dry_run: false,
            silent: false,
            file: None,
        }
    }

    #[test]
    fn parse_global_flags_and_command() {
        let cli = Cli::parse_from(["br", "--json", "-vv", "list"]);
        assert!(cli.json);
        assert_eq!(cli.verbose, 2);
        assert!(!cli.quiet);
        assert!(matches!(cli.command, Commands::List(_)));
    }

    #[test]
    fn parse_create_title_positional() {
        let cli = Cli::parse_from(["br", "create", "FixBug"]);
        match cli.command {
            Commands::Create(args) => {
                assert_eq!(args.title.as_deref(), Some("FixBug"));
            }
            other => unreachable!("expected create command, got {other:?}"),
        }
    }

    #[test]
    fn build_overrides_maps_flags() {
        let cli = Cli::parse_from([
            "br",
            "--json",
            "--no-color",
            "--no-db",
            "--no-auto-flush",
            "--lock-timeout",
            "2500",
            "list",
        ]);
        let overrides = build_cli_overrides(&cli);
        assert_eq!(overrides.json, Some(true));
        assert_eq!(overrides.display_color, Some(false));
        assert_eq!(overrides.no_db, Some(true));
        assert_eq!(overrides.no_auto_flush, Some(true));
        assert_eq!(overrides.lock_timeout, Some(2500));
    }

    #[test]
    fn build_overrides_omits_absent_startup_bool_flags() {
        let cli = Cli::parse_from(["br", "list"]);
        let overrides = build_cli_overrides(&cli);

        assert_eq!(overrides.no_db, None);
        assert_eq!(overrides.no_daemon, None);
        assert_eq!(overrides.no_auto_flush, None);
        assert_eq!(overrides.no_auto_import, None);
    }

    #[test]
    fn help_includes_core_commands() {
        let help = Cli::command().render_help().to_string();
        assert!(help.contains("create"));
        assert!(help.contains("list"));
        assert!(help.contains("sync"));
        assert!(help.contains("ready"));
    }

    #[test]
    fn version_includes_name_and_version() {
        let version = Cli::command().render_version();
        assert!(version.contains("br"));
        assert!(version.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn is_mutating_command_detects_mutations() {
        let create_cmd = Commands::Create(make_create_args());
        let list_cmd = Commands::List(beads_rust::cli::ListArgs::default());
        assert!(is_mutating_command(&create_cmd));
        assert!(!is_mutating_command(&list_cmd));
    }

    #[test]
    fn is_mutating_command_distinguishes_read_only_subcommands() {
        let dep_list = Cli::parse_from(["br", "dep", "list", "bd-123"]).command;
        let dep_add = Cli::parse_from(["br", "dep", "add", "bd-123", "bd-456"]).command;
        let label_list = Cli::parse_from(["br", "label", "list"]).command;
        let label_add = Cli::parse_from(["br", "label", "add", "bd-123", "--label", "ops"]).command;
        let comments_list = Cli::parse_from(["br", "comments", "bd-123"]).command;
        let comments_add = Cli::parse_from(["br", "comments", "add", "bd-123", "hello"]).command;

        assert!(!is_mutating_command(&dep_list));
        assert!(is_mutating_command(&dep_add));
        assert!(!is_mutating_command(&label_list));
        assert!(is_mutating_command(&label_add));
        assert!(!is_mutating_command(&comments_list));
        assert!(is_mutating_command(&comments_add));
    }

    #[test]
    fn sync_is_not_auto_imported_or_auto_flushed() {
        let sync_cmd = Cli::parse_from(["br", "sync"]).command;
        assert!(!is_mutating_command(&sync_cmd));
        assert!(!should_auto_import(&sync_cmd));
    }

    #[test]
    fn diagnostic_and_config_commands_skip_auto_import() {
        let cases: &[&[&str]] = &[
            &["br", "doctor"],
            &["br", "where"],
            &["br", "schema"],
            &["br", "config", "path"],
            &["br", "history", "list"],
            &["br", "orphans"],
        ];

        for argv in cases {
            let command = Cli::parse_from(*argv).command;
            assert!(
                !should_auto_import(&command),
                "command should not auto-import: {command:?}"
            );
        }
    }

    #[test]
    fn should_render_errors_as_json_when_command_requests_json_format() {
        let cli = Cli::parse_from(["br", "list", "--format", "json"]);
        assert!(should_render_errors_as_json_with_env(&cli, None));
    }

    #[test]
    fn should_render_errors_as_json_for_query_run_json_format() {
        let cli = Cli::parse_from(["br", "query", "run", "saved", "--format", "json"]);
        assert!(should_render_errors_as_json_with_env(&cli, None));
    }

    #[test]
    fn should_render_errors_as_json_when_command_requests_toon_format() {
        let cli = Cli::parse_from(["br", "list", "--format", "toon"]);
        assert!(should_render_errors_as_json_with_env(&cli, None));
    }

    #[test]
    fn should_render_errors_as_json_when_env_requests_json_format() {
        let cli = Cli::parse_from(["br", "history", "list"]);
        assert!(should_render_errors_as_json_with_env(
            &cli,
            Some(OutputFormat::Json)
        ));
    }

    #[test]
    fn should_render_errors_as_json_when_env_requests_toon_format() {
        let cli = Cli::parse_from(["br", "history", "list"]);
        assert!(should_render_errors_as_json_with_env(
            &cli,
            Some(OutputFormat::Toon)
        ));
    }

    #[test]
    fn should_not_render_errors_as_json_without_json_request() {
        let cli = Cli::parse_from(["br", "history", "list"]);
        assert!(!should_render_errors_as_json_with_env(&cli, None));
    }

    #[test]
    fn preopen_storage_skips_commands_without_bootstrap_or_flush_work() {
        assert!(!should_preopen_storage(true, false, false));
    }

    #[test]
    fn preopen_storage_keeps_mutating_auto_flush_path() {
        assert!(should_preopen_storage(true, false, true));
    }

    #[test]
    fn preopen_storage_keeps_bootstrap_path_for_staleness_checks() {
        assert!(should_preopen_storage(true, true, false));
    }
}
