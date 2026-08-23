//! The Spate arm of the streaming ETL benchmark, as a containerised
//! long-running process.
//!
//! This runs in a container like every other arm, including when the system
//! under test is our own: an in-process host run would get all of the host's
//! cores and make the 4-CPU envelope meaningless. Measuring ourselves outside
//! the constraints we hold competitors to would void the comparison before it
//! started.
//!
//! It never decides when to stop. The driver watches rows landing in ClickHouse
//! and removes the container, exactly as it does for Flink — so no arm gets a
//! shutdown path the others lack.
//!
//! It also does not report on itself. `metrics.exporter` is `none`: every
//! published figure comes from the driver's cgroup sampler and from ClickHouse.
//! `methodology/` is normative.
//!
//! Env:
//! - `FORMAT` (`native`) — `native` or `rowbinary`. Both are published: `native`
//!   is what a real deployment runs, `rowbinary` is the control that separates
//!   "the framework is faster" from "we chose a faster wire format".
//! - `BOOTSTRAP`, `REGISTRY_URL`, `CLICKHOUSE_URL`, `TOPIC`, `GROUP_ID`
//! - `THREADS`, `IO_THREADS`, `SHARDS`, `INFLIGHT`, `LINGER_MS`, `MAX_ROWS`,
//!   `BUDGET_MIB`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use apache_avro::types::Value as AvroValue;
use bytes::BytesMut;
use serde::Serialize;
use spate_arm::rows::{self, Row};
use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, RegistrySection};
use spate_benchmark_harness::corpus;
use spate_benchmark_harness::{env_str, env_u64};
use spate_clickhouse::{ClickHouseEncoder, Format, NativeEncoder};
use spate_core::config::{ComponentConfig, PipelineConfig};
use spate_core::deser::{Owned, RecFamily};
use spate_core::error::SinkError;
use spate_core::ops::chain;
use spate_core::pipeline::Pipeline;
use spate_core::record::Record;
use spate_core::sink::{KeyHashRouter, RowEncoder};

/// The framework version and revision this binary linked, baked in by
/// `build.rs` from `Cargo.lock`.
///
/// The driver executes the arm with `--version` and parses this, which is how a
/// result record comes to name the code it measured. `driver::parse_version`
/// does the reading: the first whitespace token starting with a digit and
/// containing a dot is the version, a parenthesised hex string is the commit,
/// and a `toolchain:` line is the compiler. Changing the shape below means
/// changing that parser.
fn version_line() -> String {
    format!(
        "spate-arm {} ({})",
        env!("SPATE_ARM_FRAMEWORK_VERSION"),
        env!("SPATE_ARM_FRAMEWORK_COMMIT"),
    )
}

fn main() {
    // Answered before anything else, and without touching the broker: the driver
    // calls this against the built image before a run to resolve provenance, so
    // it must work with no infrastructure present.
    if std::env::args().any(|a| a == "--version") {
        println!("{}", version_line());
        println!("toolchain: {}", env!("SPATE_ARM_TOOLCHAIN"));
        return;
    }

    let format = match env_str("FORMAT", "native").as_str() {
        "native" => Format::Native,
        "rowbinary" => Format::RowBinary,
        other => panic!("unknown FORMAT {other} (native|rowbinary)"),
    };

    println!("{} format={format:?}", version_line());

    run(format);
}

/// Framework sections. The connector bodies are empty here and built separately
/// below: `PipelineConfig` owns the framework knobs, and each connector parses
/// its own `ComponentConfig`.
fn pipeline_config(threads: u64, io_threads: u64, budget_mib: u64) -> PipelineConfig {
    PipelineConfig::from_str(&format!(
        "pipeline: {{ name: comparison, threads: {threads}, io_threads: {io_threads} }}\n\
         checkpoint: {{ interval: 5s, max_pending_batches: 8192 }}\n\
         backpressure: {{ max_inflight_bytes: {budget_mib}MiB }}\n\
         metrics: {{ exporter: none }}\n\
         source: {{ kafka: {{}} }}\n\
         sink: {{ clickhouse: {{}} }}\n"
    ))
    .expect("pipeline config")
}

fn kafka_source() -> spate_kafka::KafkaSource {
    let mut cfg = spate_kafka::KafkaSourceConfig::new(
        env_str("BOOTSTRAP", "spate-bench-redpanda:29092"),
        env_str("TOPIC", "comparison-sensor-batches"),
        env_str("GROUP_ID", "comparison-spate"),
    );
    // Matched to Flink's checkpoint interval so the arms pay for the same
    // at-least-once guarantee at the same cadence.
    cfg.commit_interval = Duration::from_secs(5);
    cfg.startup_timeout = Duration::from_secs(60);
    cfg.statistics_interval = Duration::from_secs(5);
    cfg.rdkafka = BTreeMap::from([(
        // `earliest` replays a prefilled corpus from the beginning (drain
        // mode). `latest` starts at the tail, which sustained mode requires:
        // with a backlog present the consumer runs flat out draining it, so
        // the measured throughput would be catch-up speed rather than the
        // rate we offered — and would read as *higher* than the offered rate,
        // which is how the mistake announces itself.
        "auto.offset.reset".to_owned(),
        env_str("OFFSET_RESET", "earliest"),
    )]);
    spate_kafka::KafkaSource::new(cfg)
}

/// The egress shape the driver's sweep varies.
#[derive(Clone, Copy, Debug)]
struct Egress {
    shards: u64,
    inflight: u64,
    linger_ms: u64,
    max_rows: u64,
}

impl Egress {
    fn from_env() -> Self {
        Self {
            shards: env_u64("SHARDS", 1).max(1),
            inflight: env_u64("INFLIGHT", 2).max(1),
            linger_ms: env_u64("LINGER_MS", 500),
            max_rows: env_u64("MAX_ROWS", 262_144).max(1),
        }
    }

    /// In-flight byte budget for this egress shape, in MiB.
    ///
    /// This has to scale with the egress shape or the sweep measures the wrong
    /// thing: `shards x inflight` sealed batches can be pending at once, and if
    /// the budget cannot hold them the pipeline backpressures on the *budget*
    /// rather than on the sink. A sweep that widened egress while leaving the
    /// budget fixed would show throughput plateauing and invite the conclusion
    /// that egress concurrency does not help, when the real limit was a number
    /// we chose.
    fn budget_mib(self) -> u64 {
        const ROW_BYTES: u64 = 128;
        const HEADROOM: u64 = 4;
        let pending = self.shards * self.inflight * self.max_rows * ROW_BYTES;
        (pending * HEADROOM / (1024 * 1024)).max(512)
    }
}

/// The sink section, built from the committed column list so the wire order
/// cannot drift from the DDL.
fn sink_section(format: Format) -> ComponentConfig {
    let url = env_str("CLICKHOUSE_URL", "http://spate-bench-clickhouse:8123");
    let columns = corpus::COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    let format_key = match format {
        Format::Native => "native",
        Format::RowBinary => "rowbinary",
        other => panic!("{}", unsupported(other)),
    };
    // Egress shape is driver-controlled, not hardcoded. These are the knobs the
    // sweep varies to find out what the arm is actually bound by; if they were
    // fixed here the sweep would vary nothing and quietly report that no
    // configuration made a difference.
    //
    // Several shards pointing at the same server is how a single-node target
    // gets concurrent inserts: each shard is an independent worker with its own
    // in-flight permits, so `shards x inflight` is the real egress concurrency.
    let Egress {
        shards,
        inflight,
        linger_ms,
        max_rows,
    } = Egress::from_env();
    let replicas = (0..shards)
        .map(|_| format!("    - replicas: [{url:?}]\n"))
        .collect::<String>();
    let yaml = format!(
        "clickhouse:\n  table: {}\n  columns: [{columns}]\n  \
         user: {}\n  password: {:?}\n  format: {format_key}\n  \
         settings: {{ async_insert: \"0\" }}\n  \
         shards:\n{replicas}  \
         inflight: {{ max_per_shard: {inflight} }}\n  \
         batch: {{ linger: {linger_ms}ms, max_rows: {max_rows} }}\n",
        corpus::TABLE,
        env_str("CLICKHOUSE_USER", "default"),
        env_str("CLICKHOUSE_PASSWORD", "bench"),
    );
    serde_yaml::from_str(&yaml).expect("sink section")
}

fn run(format: Format) {
    let threads = env_u64("THREADS", 2);
    let io_threads = env_u64("IO_THREADS", 2);
    // `BUDGET_MIB=0` (the default) derives the budget from the egress shape; an
    // explicit value overrides it, for deliberately probing budget-bound
    // behaviour.
    let budget_mib = match env_u64("BUDGET_MIB", 0) {
        0 => Egress::from_env().budget_mib(),
        explicit => explicit,
    };
    println!("egress={:?} budget={budget_mib}MiB", Egress::from_env());

    let pipeline =
        Pipeline::from_config(pipeline_config(threads, io_threads, budget_mib)).expect("pipeline");

    let avro = AvroDeserializerBuilder::from_settings(
        &AvroSettings {
            mode: AvroMode::Confluent,
            registry: Some(RegistrySection {
                url: env_str("REGISTRY_URL", "http://spate-bench-redpanda:8081"),
                username: None,
                password: None,
            }),
            ..AvroSettings::default()
        },
        &pipeline.io_handle(),
    )
    .expect("avro builder");

    let sink = spate_clickhouse::from_component_config(&sink_section(format)).expect("ch sink");
    // `native` needs the server's real column types; `rowbinary` does not.
    let native_schema = match format {
        Format::Native => Some(
            pipeline
                .block_on(sink.native_schema())
                .expect("native schema"),
        ),
        Format::RowBinary => None,
        other => panic!("{}", unsupported(other)),
    };

    // `Emitter::emit` returns a `Flow`, and every site below discards it. That
    // is deliberate, and the opposite of what it looks like:
    //
    // * No backpressure is lost. The emitter latches the signal internally
    //   (sticky once blocked) and the chain reads it after the closure returns;
    //   the return value is only an early-exit *hint*.
    // * Acting on the hint here would be a data-loss bug. A `flat_map` must emit
    //   every output of the input record it was given — the chain's resume
    //   cursor is per input record, not per fan-out element, so breaking out
    //   mid-batch would silently discard the remaining events of that message.
    //
    // One chain: `build_value` is the documented throughput path of the shipped
    // Avro deserializer, and the flatten applies the workload's filters and
    // derivations before the format-generic encoder sees a row.
    let d = avro.build_value().expect("value deserializer");
    let enc = encoder(format, native_schema);
    let report = pipeline
        .sink(sink)
        .expect("sink")
        .chains(move |ctx| {
            let (d, enc) = (d.clone(), enc.clone());
            let chunk = ctx.chunk();
            chain::<Owned<AvroValue>, _>(d)
                .with_metrics(ctx.pipeline, "main")
                .flat_map::<Owned<Row>, _>(|v, out| {
                    rows::flatten_value(&v, |row| {
                        out.emit(row);
                    });
                })
                .sink(enc, KeyHashRouter, chunk, ctx.queues, ctx.budget)
                .build()
        })
        .run(kafka_source());

    let report = report.expect("pipeline run");
    report.log();
    std::process::exit(report.exit_code());
}

/// Build the encoder for the selected wire format.
fn encoder<F: RecFamily>(
    format: Format,
    native: Option<Arc<spate_clickhouse::NativeSchema>>,
) -> EitherEncoder<F> {
    match format {
        Format::Native => EitherEncoder::Native(NativeEncoder::new(native.expect("native schema"))),
        Format::RowBinary => EitherEncoder::RowBinary(ClickHouseEncoder::new()),
        other => panic!("{}", unsupported(other)),
    }
}

/// The refusal for a wire format this arm does not publish.
fn unsupported(format: Format) -> String {
    format!("unsupported wire format {format:?} (native|rowbinary)")
}

/// One encoder type covering both wire formats.
///
/// The terminal sink stage takes a single concrete encoder type, so the `FORMAT`
/// choice has to collapse into one type rather than branching the whole chain.
/// The per-record cost is a single perfectly-predicted branch; the alternative
/// was two near-identical chain blocks, one per wire format.
enum EitherEncoder<F: RecFamily> {
    Native(NativeEncoder<F>),
    RowBinary(ClickHouseEncoder<F>),
}

// Hand-written rather than derived: `#[derive(Clone)]` would demand `F: Clone`,
// but `F` is a family marker type that is never instantiated. Both wrapped
// encoders take the same approach for the same reason.
impl<F: RecFamily> Clone for EitherEncoder<F> {
    fn clone(&self) -> Self {
        match self {
            Self::Native(e) => Self::Native(e.clone()),
            Self::RowBinary(e) => Self::RowBinary(e.clone()),
        }
    }
}

impl<F> RowEncoder<F> for EitherEncoder<F>
where
    F: RecFamily,
    for<'b> F::Rec<'b>: Serialize,
{
    fn encode<'buf>(
        &mut self,
        rec: &Record<F::Rec<'buf>>,
        buf: &mut BytesMut,
    ) -> Result<(), SinkError> {
        match self {
            Self::Native(e) => e.encode(rec, buf),
            Self::RowBinary(e) => e.encode(rec, buf),
        }
    }

    // Both must delegate: Native buffers a whole columnar block before any bytes
    // exist, and the terminal stage adds this to the shard buffer length when
    // deciding whether to seal a chunk. Returning the default 0 here would let a
    // Native block grow past the chunk's target size.
    fn buffered_bytes(&self) -> usize {
        match self {
            Self::Native(e) => e.buffered_bytes(),
            Self::RowBinary(e) => e.buffered_bytes(),
        }
    }

    // Likewise: Native's buffered rows only become a frame here, so a missing
    // delegation would silently drop the tail of every chunk.
    fn finish_chunk(&mut self, buf: &mut BytesMut) -> Result<(), SinkError> {
        match self {
            Self::Native(e) => e.finish_chunk(buf),
            Self::RowBinary(e) => e.finish_chunk(buf),
        }
    }
}
