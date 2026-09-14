//! Port of `ConsoleColors.kt`, `ColorConsoleAppender.kt`, `LogArchiver.kt`, and
//! `LineCountTriggeringPolicy.kt`.
//!
//! The JVM version needed a hand-rolled async queue to decouple `log.info(...)` calls (which
//! block synchronously on the write syscall) from Netty event-loop threads, plus a custom logback
//! appender to route lines through that queue and through JLine's `printAbove`. `tracing`'s
//! `fmt` layer already writes through a `MakeWriter`, so the same decoupling/JLine-routing is
//! implemented here as a custom `Write` sink passed to `fmt::layer().with_writer(...)`, rather
//! than needing a whole custom appender class.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};

use regex::Regex;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;

/// Cap on queued-but-not-yet-written console lines — see `ConsoleColors.kt`'s
/// `MAX_QUEUED_LINES` doc: bounded so a slow console (laggy SSH, a hosting panel scraping stdout,
/// a stuck Docker log driver) can't grow this queue without bound. Every line is still written in
/// full to `log/latest.log` via the file writer regardless of what happens here.
const MAX_QUEUED_LINES: usize = 10_000;

/// Rolls `log/latest.log` every this many lines — see `LineCountTriggeringPolicy.kt`.
const MAX_LINES_PER_FILE: u64 = 10_000;

/// How many `log/latest.N.log.gz` within-run rolls to keep — see `logback.xml`'s
/// `FixedWindowRollingPolicy` (`minIndex=1`, `maxIndex=7`).
const MAX_ROLLED_FILES: u32 = 7;

/// How many gzipped past-run logs (`<timestamp>.log.gz`) to keep — see `LogArchiver.kt`'s
/// `MAX_ARCHIVED_LOGS`.
const MAX_ARCHIVED_LOGS: usize = 7;

/// Callback registered by the console REPL once it has taken over the terminal for line editing
/// (mirrors `activeLineReader` in `ConsoleColors.kt`). When set, log lines are handed to this
/// instead of written directly, so a line logged mid-command doesn't corrupt the in-progress
/// prompt — the console module is expected to route this through `rustyline`'s external printer.
static ACTIVE_LINE_PRINTER: OnceLock<Mutex<Option<Box<dyn FnMut(&str) + Send>>>> = OnceLock::new();

fn active_line_printer() -> &'static Mutex<Option<Box<dyn FnMut(&str) + Send>>> {
    ACTIVE_LINE_PRINTER.get_or_init(|| Mutex::new(None))
}

/// Registers `printer` to receive console lines instead of a raw stdout write, once the console
/// REPL has taken over the terminal. Pass `None` to go back to plain stdout writes.
pub fn set_active_line_printer(printer: Option<Box<dyn FnMut(&str) + Send>>) {
    *active_line_printer().lock().unwrap() = printer;
}

// ANSI SGR codes. Only the basic 8/16-colour set plus bold/dim — universally supported by real
// terminals and by the log viewers hosting panels use.
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const WHITE: &str = "\x1b[37m";
const BRIGHT_RED: &str = "\x1b[91m";
const BRIGHT_YELLOW: &str = "\x1b[93m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";
const BRIGHT_CYAN: &str = "\x1b[96m";
const MAGENTA: &str = "\x1b[35m";
const GRAY: &str = "\x1b[90m";

fn level_color(level: &str) -> &'static str {
    match level {
        "ERROR" => "\x1b[1m\x1b[91m",
        "WARN" => "\x1b[1m\x1b[93m",
        "INFO" => GREEN,
        "DEBUG" => CYAN,
        "TRACE" => DIM,
        _ => WHITE,
    }
}

struct Highlight {
    pattern: Regex,
    render: fn(&str) -> String,
}

fn highlights() -> &'static [Highlight] {
    static HIGHLIGHTS: OnceLock<Vec<Highlight>> = OnceLock::new();
    HIGHLIGHTS.get_or_init(|| {
        vec![
            Highlight {
                pattern: Regex::new(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b").unwrap(),
                render: |m| format!("{DIM}{m}{WHITE}"),
            },
            Highlight {
                pattern: Regex::new(r"/?\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b").unwrap(),
                render: |m| format!("{BRIGHT_CYAN}{m}{WHITE}"),
            },
            Highlight {
                pattern: Regex::new(r"'[^']*'").unwrap(),
                render: |m| format!("{BRIGHT_YELLOW}{m}{WHITE}"),
            },
            Highlight {
                pattern: Regex::new(r" -> ").unwrap(),
                render: |_| format!(" {DIM}->{WHITE} "),
            },
            Highlight {
                pattern: Regex::new(r"\b\d[\d,]*\s?(?:ms|bytes?|KB|MB|GB|packets?|route\(s\)|player\(s\)|connection\(s\))\b").unwrap(),
                render: |m| format!("{MAGENTA}{m}{WHITE}"),
            },
        ]
    })
}

fn line_pattern() -> &'static Regex {
    static LINE_PATTERN: OnceLock<Regex> = OnceLock::new();
    LINE_PATTERN.get_or_init(|| Regex::new(r"(?s)^(\d{2}:\d{2}:\d{2}) \[(\w+)\] (.*)$").unwrap())
}

/// Reformats and colours one already-rendered "HH:mm:ss [LEVEL] message" log line as
/// "[HH:mm:ss] [LEVEL] message" with level/structure highlighting. Never panics — an
/// unrecognised line is passed through with a single plain-white wrap.
pub fn colorize(line: &str) -> String {
    let Some(caps) = line_pattern().captures(line) else {
        return format!("{WHITE}{line}{RESET}");
    };
    let time = &caps[1];
    let level = &caps[2];
    let message = &caps[3];
    let color = level_color(level);
    let mut level_tag = level.to_string();
    level_tag.truncate(5);
    while level_tag.len() < 5 {
        level_tag.push(' ');
    }

    let mut body = message.to_string();
    for h in highlights() {
        body = h
            .pattern
            .replace_all(&body, |c: &regex::Captures| (h.render)(&c[0]))
            .into_owned();
    }

    format!("{GRAY}[{time}]{RESET} {color}{level_tag}{RESET} {WHITE}{body}{RESET}")
}

/// Whether to emit ANSI colour. Order of precedence mirrors `detectColorSupport()`:
/// 1. `NO_COLOR` set (any value) -> never (<https://no-color.org>)
/// 2. `FORCE_COLOR` / `CLICOLOR_FORCE` truthy -> always
/// 3. stdout is a real terminal -> yes
/// 4. `TERM` set and not "dumb" -> yes
/// 5. otherwise -> no
pub fn detect_color_support() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if is_truthy(std::env::var("FORCE_COLOR").ok()) || is_truthy(std::env::var("CLICOLOR_FORCE").ok()) {
        return true;
    }
    if is_stdout_tty() {
        return true;
    }
    match std::env::var("TERM") {
        Ok(t) if t != "dumb" => true,
        _ => false,
    }
}

fn is_truthy(v: Option<String>) -> bool {
    matches!(v.as_deref(), Some(s) if !s.is_empty() && s != "0" && !s.eq_ignore_ascii_case("false"))
}

#[cfg(unix)]
pub fn is_stdout_tty() -> bool {
    use std::os::unix::io::AsRawFd;
    libc_isatty(io::stdout().as_raw_fd())
}
#[cfg(not(unix))]
pub fn is_stdout_tty() -> bool {
    false
}

/// Whether stdin is a real interactive terminal (as opposed to a pipe, or a hosting panel's
/// console that feeds commands in without allocating a pty - Pterodactyl's "Wings" daemon runs
/// containers with `tty: false` by default, so this is `false` there even though stdout still
/// looks fine). `console::start` uses this to decide whether `rustyline`'s line editing/redraw
/// is safe to attempt - without a real pty, its cursor-repositioning escape codes have nothing
/// to act on, producing the exact "bare `>` on its own line, typed text echoed on the next line"
/// glitch this was added to avoid; see `console.rs`'s plain-console fallback.
#[cfg(unix)]
pub fn is_stdin_tty() -> bool {
    use std::os::unix::io::AsRawFd;
    libc_isatty(io::stdin().as_raw_fd())
}
#[cfg(not(unix))]
pub fn is_stdin_tty() -> bool {
    false
}

#[cfg(unix)]
fn libc_isatty(fd: i32) -> bool {
    // Avoids pulling in the `libc` crate for one syscall; matches its `isatty` signature exactly.
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    unsafe { isatty(fd) != 0 }
}

/// Bounded, drop-on-backpressure async console writer — the `Write` sink handed to
/// `fmt::layer().with_writer(...)` for the console layer. Formatting a line and sending it here
/// never blocks on actual terminal/SSH I/O; a dedicated background thread drains the queue.
#[derive(Clone)]
pub struct ConsoleWriter {
    tx: SyncSender<String>,
    dropped_since_notice: Arc<AtomicU64>,
    use_color: bool,
}

impl ConsoleWriter {
    pub fn install() -> Self {
        let (tx, rx) = sync_channel::<String>(MAX_QUEUED_LINES);
        let dropped_since_notice = Arc::new(AtomicU64::new(0));
        let dropped_for_thread = dropped_since_notice.clone();
        std::thread::Builder::new()
            .name("console-writer".into())
            .spawn(move || {
                while let Ok(line) = rx.recv() {
                    print_console_line(&line);
                    let dropped = dropped_for_thread.swap(0, Ordering::Relaxed);
                    if dropped > 0 {
                        print_console_line(&format!(
                            "... {dropped} console line(s) dropped (console was falling behind)"
                        ));
                    }
                }
            })
            .expect("failed to spawn console-writer thread");
        Self { tx, dropped_since_notice, use_color: detect_color_support() }
    }
}

fn print_console_line(line: &str) {
    let mut guard = active_line_printer().lock().unwrap();
    match guard.as_mut() {
        Some(printer) => printer(line),
        None => println!("{line}"),
    }
}

impl Write for ConsoleWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let text = String::from_utf8_lossy(buf);
        for raw_line in text.split_inclusive('\n') {
            let line = raw_line.trim_end_matches('\n');
            if line.is_empty() {
                continue;
            }
            let formatted = if self.use_color { colorize(line) } else { line.to_string() };
            // try_send, not send: if the writer thread can't keep up, drop the line instead of
            // growing the queue without bound — see MAX_QUEUED_LINES.
            if let Err(TrySendError::Full(_)) = self.tx.try_send(formatted) {
                self.dropped_since_notice.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ConsoleWriter {
    type Writer = ConsoleWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Archives the previous run's `log/latest.log` (if any) as a timestamped `.log.gz` before the
/// file writer opens a fresh `log/latest.log` for this run, then prunes archives beyond
/// `MAX_ARCHIVED_LOGS`. Must run before logging is initialized — same ordering requirement as
/// the Kotlin version, though the reason differs: here it's just to avoid archiving the very
/// lines this run is about to write, not a file-locking race (Rust doesn't keep a locked handle
/// open the way logback's `FileAppender` does until `RollingFileWriter::open` runs).
pub fn archive_previous_log(log_dir: impl AsRef<Path>) -> io::Result<()> {
    let log_dir = log_dir.as_ref();
    fs::create_dir_all(log_dir)?;
    let latest = log_dir.join("latest.log");
    if let Ok(meta) = fs::metadata(&latest) {
        if meta.len() > 0 {
            let modified = meta.modified().unwrap_or_else(|_| std::time::SystemTime::now());
            let datetime: chrono::DateTime<chrono::Local> = modified.into();
            let timestamp = datetime.format("%Y-%m-%d_%H-%M-%S").to_string();
            let mut archive = log_dir.join(format!("{timestamp}.log.gz"));
            let mut suffix = 1;
            while archive.exists() {
                archive = log_dir.join(format!("{timestamp}-{suffix}.log.gz"));
                suffix += 1;
            }
            let input = fs::read(&latest)?;
            let file = fs::File::create(&archive)?;
            let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            encoder.write_all(&input)?;
            encoder.finish()?;
            fs::remove_file(&latest)?;
        }
    }
    prune_old_archives(log_dir)
}

fn prune_old_archives(log_dir: &Path) -> io::Result<()> {
    let mut archives: Vec<(PathBuf, std::time::SystemTime)> = fs::read_dir(log_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            e.path().is_file() && name.ends_with(".log.gz") && !name.starts_with("latest.")
        })
        .filter_map(|e| e.metadata().ok().and_then(|m| m.modified().ok()).map(|t| (e.path(), t)))
        .collect();
    archives.sort_by(|a, b| b.1.cmp(&a.1));
    for (path, _) in archives.into_iter().skip(MAX_ARCHIVED_LOGS) {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

struct RollingFileWriterInner {
    log_dir: PathBuf,
    file: Mutex<fs::File>,
    line_count: AtomicI64,
}

/// Rolling-by-line-count file writer for `log/latest.log`. Every `MAX_LINES_PER_FILE` lines,
/// shifts `latest.log` -> `latest.1.log.gz` (bumping any existing `latest.N.log.gz` up to
/// `latest.(N+1).log.gz`, oldest beyond `MAX_ROLLED_FILES` dropped) and starts a fresh
/// `latest.log` — mirrors `LineCountTriggeringPolicy.kt` + the `FixedWindowRollingPolicy` in
/// `logback.xml`. `Clone`s share the same underlying file/counter (via `Arc`), matching the
/// `Clone`-based `MakeWriter` pattern `tracing-subscriber` expects.
#[derive(Clone)]
pub struct RollingFileWriter(Arc<RollingFileWriterInner>);

impl RollingFileWriter {
    pub fn open(log_dir: impl AsRef<Path>) -> io::Result<Self> {
        let log_dir = log_dir.as_ref().to_path_buf();
        fs::create_dir_all(&log_dir)?;
        let file = fs::OpenOptions::new().create(true).append(true).open(log_dir.join("latest.log"))?;
        Ok(Self(Arc::new(RollingFileWriterInner { log_dir, file: Mutex::new(file), line_count: AtomicI64::new(0) })))
    }

    fn roll(&self) -> io::Result<()> {
        let log_dir = &self.0.log_dir;
        // Bump latest.(N).log.gz -> latest.(N+1).log.gz from the top down so nothing overwrites
        // a not-yet-moved file; drop whatever would land beyond MAX_ROLLED_FILES.
        for i in (1..MAX_ROLLED_FILES).rev() {
            let from = log_dir.join(format!("latest.{i}.log.gz"));
            let to = log_dir.join(format!("latest.{}.log.gz", i + 1));
            if from.exists() {
                if i + 1 > MAX_ROLLED_FILES {
                    let _ = fs::remove_file(&from);
                } else {
                    let _ = fs::rename(&from, &to);
                }
            }
        }
        let latest = log_dir.join("latest.log");
        let rolled_gz = log_dir.join("latest.1.log.gz");
        let input = fs::read(&latest)?;
        let gz_file = fs::File::create(&rolled_gz)?;
        let mut encoder = flate2::write::GzEncoder::new(gz_file, flate2::Compression::default());
        encoder.write_all(&input)?;
        encoder.finish()?;

        let mut guard = self.0.file.lock().unwrap();
        *guard = fs::OpenOptions::new().create(true).write(true).truncate(true).open(&latest)?;
        Ok(())
    }
}

impl Write for RollingFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        {
            let mut guard = self.0.file.lock().unwrap();
            guard.write_all(buf)?;
        }
        let newlines = buf.iter().filter(|&&b| b == b'\n').count() as i64;
        if newlines > 0 {
            let new_total = self.0.line_count.fetch_add(newlines, Ordering::Relaxed) + newlines;
            if new_total >= MAX_LINES_PER_FILE as i64 {
                self.0.line_count.store(0, Ordering::Relaxed);
                self.roll()?;
            }
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.file.lock().unwrap().flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RollingFileWriter {
    type Writer = RollingFileWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Handle for hot-reloading MCGate's own log level from config (`GateConfig.log_level`), scoped
/// to the `mcgate` crate's targets only — mirrors `applyLogLevel`'s deliberate scoping to
/// `me.hippodev`/`MCGate`, not the root logger, so `DEBUG` surfaces MCGate's own diagnostics
/// without turning on every dependency's debug noise.
pub struct LogLevelHandle {
    inner: reload::Handle<Targets, tracing_subscriber::Registry>,
}

impl LogLevelHandle {
    /// Sets the runtime log level from a config string (`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`/
    /// `OFF`). Unknown values fall back to INFO, matching `applyLogLevel`.
    pub fn apply(&self, level: &str) {
        let filter = level_filter_from_str(level);
        let _ = self.inner.modify(|targets| *targets = Targets::new().with_target("mcgate", filter));
        tracing::info!("Log level set to {}", level_name(filter));
    }
}

fn level_filter_from_str(level: &str) -> LevelFilter {
    match level.trim().to_uppercase().as_str() {
        "TRACE" => LevelFilter::TRACE,
        "DEBUG" => LevelFilter::DEBUG,
        "WARN" => LevelFilter::WARN,
        "ERROR" => LevelFilter::ERROR,
        "OFF" => LevelFilter::OFF,
        _ => LevelFilter::INFO,
    }
}

fn level_name(f: LevelFilter) -> &'static str {
    match f {
        LevelFilter::TRACE => "TRACE",
        LevelFilter::DEBUG => "DEBUG",
        LevelFilter::WARN => "WARN",
        LevelFilter::ERROR => "ERROR",
        LevelFilter::OFF => "OFF",
        _ => "INFO",
    }
}

/// Initializes console + file logging. Must be called once, before any other module logs.
/// `log_dir` is where `latest.log` and its rolls/archives live (matches the Kotlin default of
/// `log/`, relative to the working directory).
pub fn init(log_dir: impl AsRef<Path>, initial_level: &str) -> anyhow::Result<(RollingFileWriter, LogLevelHandle)> {
    let log_dir = log_dir.as_ref();
    archive_previous_log(log_dir)?;
    let file_writer = RollingFileWriter::open(log_dir)?;

    let console_writer = ConsoleWriter::install();

    // Console pattern: "HH:mm:ss [LEVEL] msg" — colorize()'s LINE_PATTERN regex-matches this
    // exact shape, same constraint as the Kotlin logback pattern.
    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(console_writer)
        .with_target(false)
        .with_level(false)
        .with_ansi(false) // colouring is applied by colorize() itself, not the fmt layer
        .without_time()
        .event_format(ConsoleTimeFormat);

    // File pattern: "yyyy-MM-dd HH:mm:ss [LEVEL] msg" — matches logback.xml's FILE encoder.
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer.clone())
        .with_target(false)
        .with_level(false)
        .with_ansi(false)
        .without_time()
        .event_format(FileTimeFormat);

    let (level_filter, reload_handle) = reload::Layer::new(Targets::new().with_target("mcgate", level_filter_from_str(initial_level)));

    tracing_subscriber::registry()
        .with(level_filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    Ok((file_writer, LogLevelHandle { inner: reload_handle }))
}

/// Custom event formatter emitting `HH:mm:ss [LEVEL] message` for the console layer, matching
/// the pattern `colorize()` expects. `tracing_subscriber`'s stock formatter doesn't produce this
/// exact shape (it puts the level first), so this is a small hand-written formatter rather than
/// a builder configuration.
struct ConsoleTimeFormat;

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for ConsoleTimeFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = chrono::Local::now().format("%H:%M:%S");
        write!(writer, "{now} [{}] ", event.metadata().level())?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Same idea as [`ConsoleTimeFormat`] but with a full date, for the file layer — matches
/// `logback.xml`'s FILE encoder pattern `%d{yyyy-MM-dd HH:mm:ss} [%level] %msg%n`.
struct FileTimeFormat;

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for FileTimeFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        write!(writer, "{now} [{}] ", event.metadata().level())?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colorize_matches_known_line() {
        let out = colorize("12:00:00 [INFO] Connection: host='play.example.com' from 1.2.3.4 (login, protocol 763)");
        assert!(out.contains("play.example.com"));
        assert!(out.contains("1.2.3.4"));
        assert!(out.starts_with(GRAY));
    }

    #[test]
    fn colorize_unrecognized_line_passes_through_plain() {
        let out = colorize("a stack trace continuation line");
        assert_eq!(out, format!("{WHITE}a stack trace continuation line{RESET}"));
    }

    #[test]
    fn no_color_env_disables_color() {
        std::env::set_var("NO_COLOR", "1");
        assert!(!detect_color_support());
        std::env::remove_var("NO_COLOR");
    }

    #[test]
    fn force_color_env_enables_color() {
        std::env::remove_var("NO_COLOR");
        std::env::set_var("FORCE_COLOR", "1");
        assert!(detect_color_support());
        std::env::remove_var("FORCE_COLOR");
    }

    #[test]
    fn archive_previous_log_moves_and_gzips_nonempty_file() {
        let dir = std::env::temp_dir().join(format!("mcgate-logtest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("latest.log"), b"hello world\n").unwrap();

        archive_previous_log(&dir).unwrap();

        assert!(!dir.join("latest.log").exists());
        let archives: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".log.gz"))
            .collect();
        assert_eq!(archives.len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn archive_previous_log_noop_when_missing_or_empty() {
        let dir = std::env::temp_dir().join(format!("mcgate-logtest-empty-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        archive_previous_log(&dir).unwrap(); // no latest.log at all
        fs::write(dir.join("latest.log"), b"").unwrap();
        archive_previous_log(&dir).unwrap(); // empty latest.log
        assert!(dir.join("latest.log").exists()); // untouched, never archived
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rolling_file_writer_rolls_after_max_lines() {
        let dir = std::env::temp_dir().join(format!("mcgate-rolltest-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut writer = RollingFileWriter::open(&dir).unwrap();
        for _ in 0..MAX_LINES_PER_FILE {
            writer.write_all(b"line\n").unwrap();
        }
        assert!(dir.join("latest.1.log.gz").exists());
        assert_eq!(fs::metadata(dir.join("latest.log")).unwrap().len(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }
}
