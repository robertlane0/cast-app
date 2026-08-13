#![forbid(unsafe_code)]

//! `ffmpeg` subprocess builder and lifecycle (`05-screen-capture.md` §4):
//! rawvideo→H.264 fMP4 encoding with the exact argument set from spec §4,
//! stdin/stdout/stderr plumbing, stderr diagnostics, and the
//! EOF → wait ≤5 s → kill teardown sequence.

use std::io::{self, BufRead};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Grace period between stdin EOF and a hard kill (spec §4.2).
pub const GRACE_PERIOD: Duration = Duration::from_secs(5);

/// Keep only the last `STDERR_TAIL` lines of `ffmpeg` stderr for diagnostics.
const STDERR_TAIL: usize = 50;

/// Poll interval while waiting for a graceful exit.
const WAIT_POLL: Duration = Duration::from_millis(50);

/// A running `ffmpeg` encoder child (`05-screen-capture.md` §4).
///
/// stdin/stdout are piped; stderr is consumed by a reader thread that
/// retains the last [`STDERR_TAIL`] lines so encode failures can be
/// diagnosed. The teardown sequence is [`Ffmpeg::wait_graceful`] after the
/// caller closes stdin (EOF finalizes the stream; a stubborn process is
/// killed).
pub struct Ffmpeg {
    child: Child,
    stderr_tail: Arc<Mutex<Vec<String>>>,
    stderr_reader: Option<JoinHandle<()>>,
}

impl Ffmpeg {
    /// Spawn `ffmpeg` from PATH encoding `width × height` raw RGBA input.
    pub fn spawn(width: u32, height: u32) -> io::Result<Self> {
        let program =
            crate::screen::ffmpeg_discover::ffmpeg_path().unwrap_or_else(|| "ffmpeg".into());
        Self::spawn_program(&program, width, height)
    }

    /// Spawn `program` with a fully custom argument vector (fake encoders in
    /// tests, where the spec §4 args are not wanted).
    pub fn spawn_custom<P: AsRef<Path>>(program: P, args: &[&str]) -> io::Result<Self> {
        let mut command = Command::new(program.as_ref());
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .args(args);
        spawn_inner(command)
    }

    /// Spawn a specific encoder program with the spec §4 argument set
    /// (tests substitute fake scripts only via [`Ffmpeg::spawn_custom`]).
    pub fn spawn_program<P: AsRef<Path>>(program: P, width: u32, height: u32) -> io::Result<Self> {
        let command = build_command(program.as_ref(), width, height);
        spawn_inner(command)
    }

    /// Take the piped stdin handle (write raw RGBA frames into it).
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    /// Take the piped stdout handle (encoded fMP4 bytes come out here).
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    /// Non-blocking exit-status probe: `Ok(Some(status))` once the child has
    /// exited, `Ok(None)` while it is still running.
    pub fn try_exit(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// The child's OS process id, if still known.
    pub fn pid(&self) -> Option<u32> {
        self.child.id().into()
    }

    /// Last [`STDERR_TAIL`] stderr lines (diagnostics for error surfacing).
    pub fn stderr_tail(&self) -> Vec<String> {
        lock_tail(&self.stderr_tail).clone()
    }

    /// The spec §4.2 teardown: wait up to `grace` for the child to exit on
    /// its own (the caller must have closed stdin so ffmpeg can finalize the
    /// stream), then kill it. Returns the final exit status.
    pub fn wait_graceful(&mut self, grace: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + grace;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(WAIT_POLL);
        }
        tracing::warn!("ffmpeg did not exit within the grace period; killing");
        let _ = self.child.kill();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(WAIT_POLL);
        }
        // `wait()` reaps if try_wait raced with the OS; on an already-reaped
        // child `wait()` errors and the try_wait loop above would have won.
        self.child.wait()
    }
}

impl Drop for Ffmpeg {
    fn drop(&mut self) {
        // Never leak a running encoder: on drop, kill whatever is left.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
    }
}

/// Common post-spawn wiring: stderr reader thread + diagnostics tail.
fn spawn_inner(mut command: Command) -> io::Result<Ffmpeg> {
    let mut child = command.spawn()?;
    let stderr = child
        .stderr
        .take()
        .expect("stderr is piped by build_command");
    let stderr_tail = Arc::new(Mutex::new(Vec::new()));
    let reader_tail = Arc::clone(&stderr_tail);
    let stderr_reader = std::thread::Builder::new()
        .name("ffmpeg-stderr".to_string())
        .spawn(move || {
            for line in io::BufReader::new(stderr).lines().map_while(Result::ok) {
                let mut tail = lock_tail(&reader_tail);
                tail.push(line);
                let overflow = tail.len().saturating_sub(STDERR_TAIL);
                tail.drain(..overflow);
            }
        })?;
    Ok(Ffmpeg {
        child,
        stderr_tail,
        stderr_reader: Some(stderr_reader),
    })
}

/// Build the encoder `Command` with the spec §4 argument set plus the
/// validated live-stream additions.
///
/// fMP4 flags: the `frag_keyframe+empty_moov` baseline writes `moov` up front
/// (spec §4.3). `-g 30` forces a keyframe every 30 frames (1 s at 30 fps):
/// x264's default keyint (250) would delay fMP4 fragments by ~8 s and stall
/// the stream. `default_base_moof` is the documented fallback if a real
/// receiver stalls at "Buffering" (AGENTS.md §12); the working set is
/// validated against a real receiver during integration.
pub(crate) fn build_command(program: &Path, width: u32, height: u32) -> Command {
    let mut command = Command::new(program);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args(encoder_args(width, height));
    command
}

/// The spec §4 encoder argument vector plus the recorded working-set
/// additions (`-g 30`; unit-tested byte-for-byte).
pub(crate) fn encoder_args(width: u32, height: u32) -> Vec<String> {
    vec![
        "-f".into(),
        "rawvideo".into(),
        "-pix_fmt".into(),
        "rgba".into(),
        "-s".into(),
        format!("{width}x{height}"),
        "-r".into(),
        "30".into(),
        "-i".into(),
        "-".into(),
        "-c:v".into(),
        "libx264".into(),
        "-preset".into(),
        "ultrafast".into(),
        "-tune".into(),
        "zerolatency".into(),
        "-g".into(),
        "30".into(),
        "-f".into(),
        "mp4".into(),
        "-movflags".into(),
        "frag_keyframe+empty_moov".into(),
        "pipe:1".into(),
    ]
}

fn lock_tail(tail: &Mutex<Vec<String>>) -> MutexGuard<'_, Vec<String>> {
    tail.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    const WIDTH: u32 = 320;
    const HEIGHT: u32 = 240;

    fn marker_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "cast-app-ffmpeg-eof-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// A fake encoder: consumes stdin until EOF, then records it and exits 0.
    fn cat_script(marker: &Path) -> String {
        format!("cat >/dev/null; touch '{}'", marker.display())
    }

    fn sh() -> PathBuf {
        PathBuf::from("sh")
    }

    #[test]
    fn encoder_args_are_the_working_set() {
        let args = encoder_args(WIDTH, HEIGHT);
        assert_eq!(
            args,
            vec![
                "-f",
                "rawvideo",
                "-pix_fmt",
                "rgba",
                "-s",
                "320x240",
                "-r",
                "30",
                "-i",
                "-",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-g",
                "30",
                "-f",
                "mp4",
                "-movflags",
                "frag_keyframe+empty_moov",
                "pipe:1",
            ]
        );
    }

    #[test]
    fn stdin_eof_is_honored_before_exit() {
        let marker = marker_path();
        let _ = std::fs::remove_file(&marker);
        let script = cat_script(&marker);
        let mut ffmpeg = Ffmpeg::spawn_custom(sh(), &["-c", &script]).unwrap();
        {
            let mut stdin = ffmpeg.take_stdin().unwrap();
            stdin.write_all(&vec![0u8; 1024]).unwrap();
            // EOF: stdin closes when `stdin` is dropped.
        }
        let status = ffmpeg.wait_graceful(Duration::from_secs(5)).unwrap();
        assert!(status.success(), "fake encoder should exit 0: {status:?}");
        assert!(marker.exists(), "script must observe stdin EOF before exit");
        let _ = std::fs::remove_file(&marker);
    }

    #[test]
    fn stubborn_process_is_killed_after_grace() {
        let started = Instant::now();
        // `sleep 300` ignores stdin entirely; graceful teardown must kill it.
        let mut ffmpeg = Ffmpeg::spawn_custom(PathBuf::from("sleep"), &["300"]).unwrap();
        let stdin = ffmpeg.take_stdin().unwrap();
        drop(stdin);
        let status = ffmpeg.wait_graceful(Duration::from_millis(500)).unwrap();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(
            !status.success(),
            "killed process must report failure: {status:?}"
        );
    }

    #[test]
    fn drop_kills_a_running_process() {
        let ffmpeg = Ffmpeg::spawn_custom(PathBuf::from("sleep"), &["300"]).unwrap();
        drop(ffmpeg);
        // Dropping must not hang: the child is killed and reaped in Drop.
    }

    #[test]
    fn stderr_lines_are_captured_with_a_tail_limit() {
        let script = "i=0; while [ $i -lt 200 ]; do echo \"line $i\" >&2; i=$((i+1)); done";
        let mut ffmpeg = Ffmpeg::spawn_custom(sh(), &["-c", script]).unwrap();
        let status = ffmpeg.wait_graceful(Duration::from_secs(10)).unwrap();
        assert!(status.success());
        if let Some(reader) = ffmpeg.stderr_reader.take() {
            reader.join().unwrap();
        }
        let tail = ffmpeg.stderr_tail();
        assert!(tail.len() <= STDERR_TAIL);
        assert!(tail.last().map(|l| l.contains("199")).unwrap_or(false));
    }
}
