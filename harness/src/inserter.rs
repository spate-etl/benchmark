//! The ceiling pass's inserter, run from **inside** the bench network.
//!
//! # The confounder this module closes
//!
//! The ClickHouse ingest ceiling used to be POSTed from the harness process on
//! macOS, through Docker Desktop's published port. Every arm inserts
//! container-to-container over [`NETWORK`] and never crosses that boundary, so
//! the two are not the same insert and the ceiling was measuring the rig.
//!
//! Four independent readings said so, all taken on this host:
//!
//! * Every format plateaued at 246–264 MB/s **on the wire** regardless
//!   of how wide its rows were — a byte wall, not a row wall.
//! * ClickHouse's own `system.query_log` attributed 84.9–85.5%
//!   of total insert duration to `NetworkReceiveElapsed`: the server
//!   was blocked reading the request body. Per insert that was 697ms of duration
//!   against 5.7ms of user and 17.3ms of system CPU.
//! * The ClickHouse container sat at 270–315% of its 500% CPU cap at the
//!   plateau. A target that is not saturated is not the thing being measured.
//! * A direct probe — same statement, same concurrency, 800 MB, `ENGINE = Null`
//!   — gave 274–279 MB/s from the host through the published port and
//!   2,598–3,126 MB/s from a container on [`NETWORK`]. Roughly ten times.
//!
//! So the inserter moved to the side of the boundary the arms are on. What it
//! costs is a container and a pipe protocol; what it buys is that the number is
//! about ClickHouse.
//!
//! # Why a Python program fed to a stock image
//!
//! The same shape [`crate::sampler`] uses, and for the same reason: a
//! measurement should not need a purpose-built image, because an image is a
//! thing to build, to version and to get wrong. `python:3.12-alpine` is pulled
//! once and nothing about it is under measurement.
//!
//! The one departure from the sampler is where the program travels. The sampler
//! feeds its source on stdin and `python3 -` reads stdin until EOF, which leaves
//! no stdin for anything else. This inserter needs stdin twice over — once for
//! tens of megabytes of pre-encoded blocks and then for a command per rung of
//! the sweep — so the program goes in `python3 -c` and stdin carries only data.
//!
//! Python is not the constraint, which was checked rather than assumed. With the
//! request shape below, against `ENGINE = Null` so the server does nothing but
//! read, this program reached 3.3–4.2 GB/s from a container on [`NETWORK`] at
//! 8–32 concurrent inserters: an order of magnitude above the wall the host-side
//! rig hit, and above the throughput the target itself sustains once it has to
//! parse, sort, compress and write what it is sent.
//!
//! # What is deliberately unchanged
//!
//! The request is byte-for-byte the shape [`crate::ceiling`]'s host-side
//! inserter sent: SQL in the query string, the block raw in the body, no
//! compression, `Connection: close`, one connection per POST. A diagnostic pass
//! with `--compression` is the one exception: it sends `async_insert=0` on every
//! POST, as every arm does, and unless its codec is `none` its bodies are
//! compressed frames sent with `decompress=1`. Persistent
//! connections were measured beside it and came out inside the noise (3.4–3.8
//! GB/s against 3.3–4.2), so keeping the old shape costs nothing and buys the
//! one thing worth having — the only variable that changed between the figure
//! before this module and the figure after it is which side of Docker's network
//! boundary the bytes came from.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use crate::ceiling::Block;
use crate::docker::{NETWORK, docker_try};
use crate::infra::Endpoints;

/// The inserter container's name. Fixed, so an orphan from an interrupted pass
/// is removed by the next one rather than accumulating — and so that a
/// half-finished pass cannot leave a container POSTing into the table the next
/// measurement is about to time.
const INSERTER_CONTAINER: &str = "spate-bench-inserter";

/// Image the inserter runs in. Chosen only because it has a Python interpreter.
const INSERTER_IMAGE: &str = "python:3.12-alpine";

/// Seconds one POST may take before the inserter gives up on it.
///
/// The same bound the host-side inserter used. It exists so that a target which
/// stops answering fails a rung instead of hanging a pass that holds the arm
/// lock.
const POST_TIMEOUT_S: u64 = 120;

/// What one rung POSTed, as the inserter counted it.
///
/// Counted inside the container, on the container's own monotonic clock, because
/// the window is a property of the inserting threads rather than of the process
/// that asked for them. A harness-side clock would additionally charge the rung
/// for the pipe round trip that starts and ends it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RemoteBurst {
    /// Rows POSTed in inserts the server answered with a 200.
    pub rows: u64,
    /// Bytes of block body those inserts carried.
    pub bytes: u64,
    /// Seconds the window ran for, from the barrier to the last completed POST.
    pub elapsed_s: f64,
}

/// A running inserter container, driven one rung at a time over a pipe.
///
/// The sweep stays in [`crate::ceiling`]. This type is deliberately not clever:
/// it holds a pre-encoded pool, and it POSTs it at whatever concurrency it is
/// told to for whatever window it is told to. The rule that decides when a rung
/// is a plateau rather than a floor is tested without a server, and moving it
/// into a Python program shipped over stdin would put it somewhere no test can
/// reach.
#[derive(Debug)]
pub struct Inserter {
    child: Child,
    /// Held open for the life of the inserter: the pool goes down it first, then
    /// one command per rung. Dropping it is how the container is told to exit.
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stopped: bool,
}

impl Inserter {
    /// Starts an inserter on [`NETWORK`] and hands it the pre-encoded pool.
    ///
    /// The pool travels **before** any rung is timed, which is the same rule the
    /// host-side inserter obeyed: encoding is the rig's work and must not land
    /// inside a window that claims to describe the target's. Handing the blocks
    /// over rather than generating them in the container also keeps one encoder
    /// in this repository — the one `harness/tests/native_encoder_matches_clickhouse.rs`
    /// proves against a live server. A second encoder written in Python would be
    /// a second thing to prove.
    ///
    /// `settings` are added to every POST's query string as `name=value`, for
    /// example `decompress=1` for a pool whose bodies are ClickHouse compressed
    /// frames. Names and values must be URL-safe as they stand.
    ///
    /// # Errors
    ///
    /// If the container will not start, if the pipe closes while the pool is
    /// being handed over, or if the program answers with anything but its ready
    /// line.
    pub fn start(
        ep: &Endpoints,
        sql: &str,
        pool: &[Block],
        settings: &[(&str, &str)],
    ) -> Result<Self, String> {
        let query_settings = query_suffix(settings);
        // An orphan from an interrupted pass would hold the name.
        let _ = docker_try(&["rm", "-f", INSERTER_CONTAINER]);

        let port = ep.ch_internal_port.to_string();
        let timeout = POST_TIMEOUT_S.to_string();
        let mut child = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-i",
                "--name",
                INSERTER_CONTAINER,
                // The whole point of this module.
                "--network",
                NETWORK,
                INSERTER_IMAGE,
                "python3",
                "-c",
                INSERTER_SRC,
                &ep.ch_container,
                &port,
                &ep.ch_user,
                &ep.ch_password,
                sql,
                &timeout,
                &query_settings,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Inherited rather than piped: a Python traceback is the first thing
            // an operator needs and the last thing that should be buffered
            // inside a struct nobody reads.
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn the inserter container: {e}"))?;

        let stdin = child.stdin.take().ok_or("the inserter has no stdin")?;
        let stdout = BufReader::new(child.stdout.take().ok_or("the inserter has no stdout")?);
        // Assembled before the pool is written, so that a failure part-way
        // through the hand-over drops a value that removes the container rather
        // than leaking one holding a pipe.
        let mut inserter = Self {
            child,
            stdin: Some(stdin),
            stdout,
            stopped: false,
        };
        write_pool(
            inserter.stdin.as_mut().ok_or("the inserter has no stdin")?,
            pool,
        )
        .map_err(|e| format!("hand the pool to the inserter: {e}"))?;

        let ready = inserter.reply()?;
        if !ready.starts_with("READY") {
            return Err(format!(
                "the inserter answered {ready:?} instead of READY. It POSTs from inside \
                 {NETWORK}, so a failure here is the container or the image rather than \
                 the target."
            ));
        }
        Ok(inserter)
    }

    /// Runs one rung: `concurrency` threads POSTing the pool for `window`.
    ///
    /// # Errors
    ///
    /// If the inserter cannot be reached, or if any of its threads failed — a
    /// rung with a refused insert in it is never reported as absorbed rows,
    /// because a ceiling inflated by inserts the server rejected is the most
    /// expensive possible number to publish.
    pub fn burst(&mut self, concurrency: u64, window: Duration) -> Result<RemoteBurst, String> {
        let command = format!("RUN {} {:.3}\n", concurrency.max(1), window.as_secs_f64());
        let stdin = self.stdin.as_mut().ok_or("the inserter has been stopped")?;
        stdin
            .write_all(command.as_bytes())
            // Flushed rather than left to the pipe's own buffering: the reply
            // below is read synchronously, so an unflushed command is a deadlock
            // rather than a delay.
            .and_then(|()| stdin.flush())
            .map_err(|e| format!("ask the inserter for a rung: {e}"))?;
        parse_burst(&self.reply()?)
    }

    /// One line from the inserter.
    fn reply(&mut self) -> Result<String, String> {
        let mut line = String::new();
        match self.stdout.read_line(&mut line) {
            Ok(0) => Err(
                "the inserter exited without answering. Its stderr is on this terminal.".to_owned(),
            ),
            Ok(_) => Ok(line.trim().to_owned()),
            Err(e) => Err(format!("read from the inserter: {e}")),
        }
    }

    /// Stops the inserter, exactly once.
    ///
    /// Removes the container by name rather than killing the `docker` client:
    /// killing the client detaches it and leaves the container alive, which
    /// [`crate::sampler`] documents having paid for.
    fn shutdown(&mut self) {
        if std::mem::replace(&mut self.stopped, true) {
            return;
        }
        drop(self.stdin.take());
        let _ = docker_try(&["rm", "-f", INSERTER_CONTAINER]);
        let _ = self.child.wait();
    }
}

impl Drop for Inserter {
    /// Every path out of a ceiling pass ends the container, including the
    /// refusals. A pass that abandoned a sweep — a rung the rig could not drive,
    /// a target that stopped answering — would otherwise leave an inserter
    /// POSTing into the table the next measurement is about to time.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Frames the pool for the pipe: a count, then a header line and the bytes for
/// each block.
///
/// Length-prefixed rather than delimited because a block is arbitrary binary and
/// contains every byte a delimiter could be made of. Written into a `Write` so
/// the framing can be exercised without a container — the parsing side is
/// Python and the two have to agree, so the side that can be tested is.
fn write_pool(out: &mut impl Write, pool: &[Block]) -> std::io::Result<()> {
    writeln!(out, "POOL {}", pool.len())?;
    for block in pool {
        writeln!(out, "BLOCK {} {}", block.rows, block.body.len())?;
        out.write_all(&block.body)?;
    }
    out.flush()
}

/// Reads one `OK rows bytes elapsed` reply, or turns an `ERR` into a refusal.
fn parse_burst(line: &str) -> Result<RemoteBurst, String> {
    let mut fields = line.split_whitespace();
    match fields.next() {
        Some("OK") => {
            let mut next = |what: &str| -> Result<String, String> {
                fields
                    .next()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("the inserter's reply carries no {what}: {line:?}"))
            };
            let rows: u64 = next("row count")?
                .parse()
                .map_err(|e| format!("the inserter's row count does not parse: {e}"))?;
            let bytes: u64 = next("byte count")?
                .parse()
                .map_err(|e| format!("the inserter's byte count does not parse: {e}"))?;
            let elapsed_s: f64 = next("window")?
                .parse()
                .map_err(|e| format!("the inserter's window does not parse: {e}"))?;
            if rows == 0 || elapsed_s <= 0.0 {
                return Err(format!(
                    "REFUSED: the inserter reported {rows} rows over {elapsed_s}s, which is \
                     not a measurement of anything"
                ));
            }
            Ok(RemoteBurst {
                rows,
                bytes,
                elapsed_s,
            })
        }
        // Passed through verbatim. The text is the server's own refusal in every
        // case that matters — a rejected insert, a target that stopped answering
        // — and rewording it here would cost the caller the one string that says
        // which of those happened.
        Some("ERR") => Err(line.trim_start_matches("ERR").trim().to_owned()),
        _ => Err(format!("the inserter answered {line:?}")),
    }
}

/// The inserter program.
///
/// Inline rather than in `workload/`, because it is not part of the workload:
/// it is this module's measurement rig, and the fidelity argument in the module
/// docs rests on its request being the same shape as the host-side inserter's.
/// Keeping the claim and the code that has to satisfy it in one file is what
/// makes that checkable by reading.
///
/// The settings as the query-string suffix every POST carries.
fn query_suffix(settings: &[(&str, &str)]) -> String {
    settings
        .iter()
        .map(|(name, value)| format!("&{name}={value}"))
        .collect()
}

/// The protocol, in full. All lines are ASCII and terminated by `\n`.
///
/// * In: `POOL <blocks>`, then per block `BLOCK <rows> <bytes>` followed by
///   exactly that many bytes of body.
/// * Out: `READY <blocks> <bytes>` once the pool is held.
/// * In: `RUN <concurrency> <seconds>`; out: `OK <rows> <bytes> <elapsed_s>` or
///   `ERR <text>`.
/// * In: `QUIT`, or EOF, and the program exits.
const INSERTER_SRC: &str = r##"
import socket, sys, threading, time, urllib.parse

host, port, user, password, sql, timeout, query_settings = (
    sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5], float(sys.argv[6]),
    sys.argv[7])

quote = lambda s: urllib.parse.quote(s, safe="")
path = "/?user=%s&password=%s&query=%s" % (quote(user), quote(password), quote(sql)) + query_settings
# Concatenated per POST rather than formatted into a template. The path is
# percent-encoded, so it is full of % signs and any %-formatting applied to a
# string containing it dies on the first one — as this did, with
# "unsupported format character 'I'" out of %20INSERT.
head_up_to_length = ("POST " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n"
                     "Content-Type: application/octet-stream\r\nContent-Length: ").encode()
head_after_length = b"\r\n\r\n"

inp = sys.stdin.buffer

def command():
    line = inp.readline()
    if not line:
        raise SystemExit(0)
    return line.decode().split()

def reply(text):
    sys.stdout.write(text + "\n")
    sys.stdout.flush()

def fail(text):
    reply("ERR " + " ".join(text.split())[:400])

header = command()
if len(header) != 2 or header[0] != "POOL":
    fail("expected a POOL header, got %r" % (header,))
    raise SystemExit(1)
pool = []
held = 0
for _ in range(int(header[1])):
    field = command()
    rows, size = int(field[1]), int(field[2])
    body = inp.read(size)
    if body is None or len(body) != size:
        fail("the pool was cut short")
        raise SystemExit(1)
    pool.append((rows, body))
    held += size
reply("READY %d %d" % (len(pool), held))

def post(body):
    s = socket.create_connection((host, port), timeout)
    s.settimeout(timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        s.sendall(head_up_to_length + str(len(body)).encode() + head_after_length)
        s.sendall(body)
        chunks = []
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        s.close()
    resp = b"".join(chunks)
    status = resp.split(b"\r\n", 1)[0]
    # A refused insert is never counted as absorbed rows.
    #
    # The BODY is reported, not the first bytes of the response. Truncating the
    # whole response to 400 bytes never got past ClickHouse's own headers, so a
    # `DB::Exception` naming MEMORY_LIMIT_EXCEEDED reached `target_refused` as a
    # header block and was classified as a limit of this rig instead of a limit
    # of the target — the two are opposite findings, and the rig had never once
    # classified one correctly.
    if b" 200 " not in status or b"DB::Exception" in resp:
        body = resp.split(b"\r\n\r\n", 1)[-1]
        detail = (body if body.strip() else resp)[:2000]
        raise RuntimeError(
            status.decode("utf-8", "replace")
            + " | "
            + detail.decode("utf-8", "replace")
        )

def run(concurrency, window):
    barrier = threading.Barrier(concurrency)
    stats = [None] * concurrency
    errors = []

    def worker(slot):
        try:
            # Every thread starts together, so the window measures the
            # concurrency it names rather than a ragged ramp into it.
            barrier.wait()
            start = time.monotonic()
            rows = sent = rounds = 0
            last = start
            while time.monotonic() - start < window:
                # Strided by the round alone: a stride of slot + round * threads
                # degenerates to a constant whenever the concurrency is a
                # multiple of the pool size, and the ladder would then compare
                # block mixtures as well as concurrencies.
                block_rows, body = pool[(slot + rounds) % len(pool)]
                post(body)
                rows += block_rows
                sent += len(body)
                rounds += 1
                last = time.monotonic()
            stats[slot] = (rows, sent, last - start)
        except BaseException as e:
            errors.append("%s: %s" % (type(e).__name__, e))
            # Releases every thread still waiting to start, so one refusal ends
            # the rung instead of hanging it.
            barrier.abort()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        return "ERR " + " ".join(errors[0].split())[:400]
    rows = sum(s[0] for s in stats)
    sent = sum(s[1] for s in stats)
    return "OK %d %d %.6f" % (rows, sent, max(s[2] for s in stats))

while True:
    field = command()
    if not field or field[0] == "QUIT":
        break
    if field[0] == "RUN":
        reply(run(int(field[1]), float(field[2])))
    else:
        fail("unknown command %r" % (field[0],))
"##;

#[cfg(test)]
mod tests {
    use super::*;

    /// Each setting reaches the POST path as its own query parameter, after the
    /// statement, and none means no suffix at all.
    #[test]
    fn settings_become_query_parameters() {
        assert_eq!(query_suffix(&[]), "");
        assert_eq!(
            query_suffix(&[("async_insert", "0"), ("decompress", "1")]),
            "&async_insert=0&decompress=1",
        );
    }

    fn block(rows: u64, body: &[u8]) -> Block {
        Block {
            body: body.to_vec(),
            rows,
        }
    }

    /// The framing has to survive a block that contains its own delimiters,
    /// which every Native block does: a length prefix is the only thing that
    /// can carry arbitrary binary down a line-oriented pipe.
    #[test]
    fn a_pool_is_framed_as_a_header_line_and_the_exact_bytes_that_follow_it() {
        let mut out = Vec::new();
        write_pool(
            &mut out,
            &[block(3, b"\nBLOCK 9 9\n"), block(2, &[0u8, 0xff, b'\n'])],
        )
        .expect("framing a pool");
        assert_eq!(
            out,
            b"POOL 2\nBLOCK 3 11\n\nBLOCK 9 9\nBLOCK 2 3\n\x00\xff\n".to_vec()
        );
    }

    #[test]
    fn a_burst_reply_carries_the_rows_the_bytes_and_the_window_they_took() {
        let burst = parse_burst("OK 38000000 3458000000 8.004321").expect("a well-formed reply");
        assert_eq!(burst.rows, 38_000_000);
        assert_eq!(burst.bytes, 3_458_000_000);
        assert!((burst.elapsed_s - 8.004_321).abs() < 1e-9);
    }

    /// A rung with a refused insert in it is a refusal, not a smaller number.
    /// The server's own text is passed through because "TOO_MANY_PARTS" and "the
    /// rig could not open another socket" are different findings and only one of
    /// them is about the target.
    #[test]
    fn an_inserter_that_reports_an_error_fails_the_rung_rather_than_reporting_what_landed() {
        let e = parse_burst("ERR RuntimeError: Code: 252. DB::Exception: Too many parts")
            .expect_err("an ERR reply is a refusal");
        assert!(e.contains("Too many parts"), "{e}");
        assert!(parse_burst("").is_err());
        assert!(parse_burst("READY 8 72000000").is_err());
        assert!(
            parse_burst("OK 1").is_err(),
            "a truncated reply is not a burst"
        );
        assert!(parse_burst("OK nine 1 1.0").is_err());
    }

    /// Zero rows over zero seconds is arithmetic, not a measurement, and it is
    /// exactly what a rung whose every insert failed would otherwise report.
    #[test]
    fn a_reply_of_no_rows_or_no_window_is_refused_rather_than_divided_by() {
        assert!(parse_burst("OK 0 0 8.0").is_err());
        assert!(parse_burst("OK 100 100 0.0").is_err());
    }
}
