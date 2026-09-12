package dev.kainth.spatebench.flink;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The Flink arm of the cross-framework comparison: consume the Confluent-framed
 * Avro topic, flatten each message's {@code events} array through the workload's
 * filters and derivations, insert one row per surviving event into ClickHouse.
 *
 * <p>{@code methodology/} is normative and this job conforms to
 * it. Three consequences worth stating where they are easy to check:
 *
 * <ul>
 *   <li><b>No instrumentation.</b> Nothing here counts, times or reports anything
 *       for the benchmark's benefit. Throughput comes from {@code SELECT count()},
 *       CPU and memory from cgroup v2, latency from {@code ingest_ts - send_ts}
 *       computed inside ClickHouse. The Flink and connector metrics that do exist
 *       are the ones they ship in production and are not read as results.</li>
 *   <li><b>No self-termination.</b> The source is unbounded and the job runs until
 *       the driver removes the container, exactly as every other arm does.</li>
 *   <li><b>Framework internals are not hand-written.</b> Decoding goes through
 *       Flink's own {@link ConfluentRegistryAvroDeserializationSchema} and the sink
 *       through ClickHouse's own connector. Tuning lives in {@code config.yaml} and
 *       in the env-driven sink batch settings below, because configuration is not
 *       code we wrote.</li>
 * </ul>
 *
 * <p>Environment:
 * <ul>
 *   <li>{@code BOOTSTRAP}, {@code TOPIC}, {@code GROUP_ID}, {@code STARTING_OFFSETS}
 *       ({@code earliest}|{@code committed}).</li>
 *   <li>{@code REGISTRY_URL}.</li>
 *   <li>{@code CLICKHOUSE_URL}, {@code CLICKHOUSE_USER}, {@code CLICKHOUSE_PASSWORD},
 *       {@code CLICKHOUSE_DATABASE}, {@code CLICKHOUSE_TABLE} (default
 *       {@code sensor_events}).</li>
 *   <li>{@code SINK_MAX_BATCH_ROWS}, {@code SINK_MAX_BUFFERED_ROWS},
 *       {@code SINK_MAX_BATCH_BYTES}, {@code SINK_LINGER_MS},
 *       {@code SINK_MAX_IN_FLIGHT}, {@code SINK_MAX_ROW_BYTES} — the sink's batch
 *       shape. See README.md for why each default is what it is.</li>
 *   <li>{@code EXPECT_PARALLELISM} — an <em>assertion</em>, never a setting. See
 *       {@link #assertParallelism}.</li>
 * </ul>
 *
 * <p>Parallelism, object reuse, buffer timeout, checkpoint interval and mode, and
 * all memory sizing are deliberately <em>not</em> set here: they live in
 * {@code config.yaml} so a reviewer can read the whole tuning surface in one file
 * without decompiling a jar. The driver reaches {@code parallelism.default} at run
 * time through the official image's own {@code FLINK_PROPERTIES} contract rather
 * than through a {@code setParallelism} call here, so that tuning it stays
 * configuration — which rule 1 permits without limit — instead of becoming code
 * we wrote that silently overrides the file a reviewer was sent to read.
 */
public final class ComparisonJob {

    private ComparisonJob() {}

    public static void main(String[] args) throws Exception {
        final String startingOffsets =
                Cfg.oneOf("STARTING_OFFSETS", "earliest", "earliest", "committed");

        final String schemaJson = SensorBatchSchema.json();
        final Schema schema = SensorBatchSchema.parse(schemaJson);
        // Fail at submission if the committed schema no longer matches the
        // positional constants, rather than at the first record on a task manager.
        SensorBatchSchema.assertFieldOrder(schema);

        final String table = Cfg.str("CLICKHOUSE_TABLE", "sensor_events");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertParallelism(env);

        System.out.printf(
                "flink arm: table=%s startingOffsets=%s format=%s parallelism=%d%n",
                table,
                startingOffsets,
                ClickHouseFormat.RowBinaryWithNamesAndTypes,
                env.getParallelism());

        final KafkaSource<GenericRecord> source = kafkaSource(schema, startingOffsets);

        pipeline(env, source, sink(SensorRow.class, new SensorRowMapper(), table), schemaJson, table);

        env.execute("comparison-flink");
    }

    /** Assemble the same graph in production and in topology/type tests. */
    static void pipeline(StreamExecutionEnvironment env, KafkaSource<GenericRecord> source,
                         Sink<SensorRow> sink, String schemaJson, String table) {
        final DataStreamSource<GenericRecord> batches =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-sensor-batches");
        batches.uid("kafka-sensor-batches");
        // Inheriting the same width permits forward edges; the generated job graph
        // determines chaining. Encoding and checkpoint serialization still happen.
        // Specific records implement GenericRecord, but their PojoTypeInfo fails
        // Flink's reflective input-type validation against that interface. Supply
        // the unchanged output POJO type through the public flatMap overload.
        batches.flatMap(new FlattenEvents(schemaJson),
                        TypeInformation.of(SensorRow.class))
                .name("flatten-events")
                .uid("flatten-events")
                .sinkTo(sink)
                .name("clickhouse-" + table)
                .uid("clickhouse-sink");
    }

    /**
     * Refuses the job when the parallelism the cluster resolved is not the one the driver asked
     * for.
     *
     * <p>{@code EXPECT_PARALLELISM} sets nothing. Parallelism is configuration and is set as
     * configuration: the driver writes {@code parallelism.default} and
     * {@code taskmanager.numberOfTaskSlots} into {@code config.yaml} through the official image's
     * {@code FLINK_PROPERTIES} and {@code TASK_MANAGER_NUMBER_OF_TASK_SLOTS} variables, which the
     * entrypoint applies with Flink's own config parser before either process starts.
     *
     * <p>This exists because that mechanism can stop working without anything looking wrong. If a
     * base-image bump renamed the hook, or the variable were dropped from the descriptor, every
     * cell of a parallelism sweep would run at whatever {@code config.yaml} ships while every
     * record claimed the value that was asked for — and the sweep would conclude, with dozens of
     * consistent measurements behind it, that parallelism does not affect this arm. That is the
     * one failure this benchmark cannot afford: not a crash, but a plausible number that is
     * quietly false. A mismatch here fails at job submission, before a single row is consumed.
     *
     * <p>The image's own default is declared in the {@code Dockerfile} and must equal
     * {@code parallelism.default} in {@code config.yaml}, so a container run by hand asserts the
     * truth about itself rather than being exempt; {@code entrants_are_valid} checks that the two
     * files agree.
     */
    private static void assertParallelism(StreamExecutionEnvironment env) {
        final int expected = Cfg.i("EXPECT_PARALLELISM", 0);
        if (expected <= 0) {
            // Absent is a failure and not a licence to skip the check. A check
            // that disables itself when its input goes missing is not a check.
            throw new IllegalStateException(
                    "EXPECT_PARALLELISM is unset or not positive. It is not optional: without it"
                            + " nothing verifies that the parallelism this job runs at is the one"
                            + " it was configured with, and a sweep would record values it never"
                            + " ran at. The image sets a default matching config.yaml; the driver"
                            + " sets it from the descriptor's `parallelism` knob.");
        }
        final int actual = env.getParallelism();
        if (actual != expected) {
            throw new IllegalStateException(
                    "EXPECT_PARALLELISM="
                            + expected
                            + " but the cluster resolved parallelism.default="
                            + actual
                            + ". The driver sets parallelism through FLINK_PROPERTIES and"
                            + " TASK_MANAGER_NUMBER_OF_TASK_SLOTS; if that no longer reaches"
                            + " config.yaml, this job would run at the image's default while"
                            + " every record claimed the value that was asked for.");
        }
    }

    // OffsetResetStrategy is deprecated in kafka-clients 4.x (superseded by
    // AutoOffsetResetStrategy), but OffsetsInitializer.committedOffsets in
    // flink-connector-kafka 5.0.0-2.2 still takes only that type — there is no
    // non-deprecated way to express "resume, else earliest" through the connector's
    // public API. Suppressed rather than avoided so the STARTING_OFFSETS=committed
    // path keeps Spate's exact semantics available.
    static KafkaSource<GenericRecord> kafkaSource(Schema schema, String startingOffsets) {
        return kafkaSource(schema, startingOffsets,
                Cfg.oneOf("AVRO_MODE", "generic", "generic", "specific"));
    }

    @SuppressWarnings({"deprecation", "unchecked", "rawtypes"})
    static KafkaSource<GenericRecord> kafkaSource(Schema schema, String startingOffsets, String avroMode) {

        final String registryUrl = Cfg.str("REGISTRY_URL", "http://spate-bench-redpanda:8081");

        // The pipeline needs only the Kafka value. The value-only adapter passes
        // that payload to the Avro deserializer; Kafka still creates ConsumerRecords.
        //
        // The produced type is GenericRecordAvroTypeInfo, so a chain break here
        // would cost Avro serialization rather than Kryo — but it would still
        // serialize the schema-resolved record on every hop, which is why the job
        // is one chain.
        // SpecificRecordBase implements GenericRecord, so the same application
        // transform handles both shipped deserializers. The generated classes
        // come from the canonical schema, never a second workload definition.
        final DeserializationSchema<GenericRecord> valueSchema =
                "specific".equals(avroMode)
                ? (DeserializationSchema) ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        rs.etl.bench.SensorBatch.class, registryUrl)
                : ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, registryUrl);

        final OffsetsInitializer offsets = "committed".equals(startingOffsets)
                // Same semantics as the Spate arm's auto.offset.reset=earliest with a
                // stable group id: resume where the group left off, else start at 0.
                ? OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST)
                // Default. A drain measurement has to replay the same corpus every
                // run, and this is also flink-connector-kafka's own default.
                : OffsetsInitializer.earliest();

        return KafkaSource.<GenericRecord>builder()
                .setBootstrapServers(Cfg.str("BOOTSTRAP", "spate-bench-redpanda:29092"))
                .setTopics(Cfg.str("TOPIC", "comparison-sensor-batches"))
                .setGroupId(Cfg.str("GROUP_ID", "comparison-flink"))
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(valueSchema)
                // The topic's partition count is fixed for the whole comparison, so
                // rediscovery would only add a metadata request every 5 minutes.
                .setProperty("partition.discovery.interval.ms", "-1")
                .setProperty("max.poll.records", Integer.toString(Cfg.i("KAFKA_MAX_POLL_RECORDS", 500)))
                .setProperty("max.partition.fetch.bytes", Integer.toString(Cfg.i("KAFKA_MAX_PARTITION_FETCH_BYTES", 1_048_576)))
                .setProperty("fetch.max.bytes", Integer.toString(Cfg.i("KAFKA_FETCH_MAX_BYTES", 52_428_800)))
                .build();
    }

    private static <T> ClickHouseAsyncSink<T> sink(
            Class<T> inputType, DataMapper<T> mapper, String table) {

        final ClickHouseClientConfig clientConfig = new ClickHouseClientConfig(
                Cfg.str("CLICKHOUSE_URL", "http://spate-bench-clickhouse:8123"),
                Cfg.str("CLICKHOUSE_USER", "default"),
                Cfg.str("CLICKHOUSE_PASSWORD", "bench"),
                Cfg.str("CLICKHOUSE_DATABASE", "default"),
                table);

        // Matches the Spate arm's `settings: { async_insert: "0" }`, and it is NOT
        // belt and braces: the server this suite measures against defaults
        // `async_insert=1`, so an arm that did not pin it would be running a
        // different experiment from the one next to it on the chart.
        //
        // Three things change under an asynchronous insert, and all three flatter
        // whichever arm gets them. The INSERT returns once the rows are buffered
        // rather than once they are written, so the sink's back-pressure signal
        // stops describing the target. `written_rows` comes back as 0, so nothing
        // downstream can tell a landed batch from a queued one. And the write is
        // charged to a background flush that leaves no `system.query_log` row, so
        // the server-side CPU-per-row figure METHODOLOGY publishes would be
        // systematically smaller for identical work. The durability promise is
        // weaker too — buffered rows are lost on a server restart — and the
        // methodology compares guarantee for guarantee.
        //
        // Set explicitly on both arms rather than left to the server, so that a
        // ClickHouse upgrade cannot move the comparison under either of them.
        final Map<String, String> serverSettings = new LinkedHashMap<>();
        serverSettings.put("async_insert", "0");
        clientConfig.setServerSettings(serverSettings);
        clientConfig.setOptions(Map.of(
                "max_open_connections", Integer.toString(Cfg.i("CLICKHOUSE_MAX_CONNECTIONS", 10)),
                "client_network_buffer_size", Integer.toString(Cfg.i("CLICKHOUSE_NETWORK_BUFFER_BYTES", 300_000))));

        // Typed (POJO) mode. The connector forces RowBinaryWithNamesAndTypes here and
        // ignores setClickHouseFormat, so the format is not set: passing anything else
        // would only produce a warning. The alternative shipped path is String mode,
        // which would mean building CSV or JSONEachRow text per row and moving work to
        // the server. That alternative path has not been measured here.
        final ClickHouseConvertor<T> convertor = new ClickHouseConvertor<>(inputType, mapper);

        final int maxBatchRows = Cfg.i("SINK_MAX_BATCH_ROWS", 50_000);
        final int maxBufferedRows = Cfg.i("SINK_MAX_BUFFERED_ROWS", 100_000);
        if (maxBufferedRows <= maxBatchRows) {
            // AsyncSinkWriter enforces this, but its message names neither knob.
            //
            // The LAST line of defence rather than the first. A combination this
            // rejects is refused by the harness before a container starts — the
            // rule is declared in `[[constraints]]` in entrant.toml and applied by
            // `bench run`, because a sweep walks the product of these two knobs and
            // will reach the impossible cell, and discovering it here costs two
            // container starts and a JVM per cell. This copy exists because the
            // image can also be run by hand, and because a knob that is capped in
            // silence is worse than one that refuses: an unreachable
            // SINK_MAX_BUFFERED_ROWS is precisely what held this arm's insert batch
            // below 50,000 rows while the arm beside it used 262,144.
            throw new IllegalArgumentException(
                    "SINK_MAX_BUFFERED_ROWS (" + maxBufferedRows
                            + ") must be strictly greater than SINK_MAX_BATCH_ROWS ("
                            + maxBatchRows
                            + "). Raise the `buffered_rows` knob with `max_rows`: it also"
                            + " bounds this subtask's checkpoint state and its retained"
                            + " payload memory.");
        }

        return ClickHouseAsyncSink.<T>builder()
                .setElementConverter(convertor)
                .setClickHouseClientConfig(clientConfig)
                .setMaxBatchSize(maxBatchRows)
                .setMaxBufferedRequests(maxBufferedRows)
                .setMaxBatchSizeInBytes(Cfg.l("SINK_MAX_BATCH_BYTES", 16L * 1024 * 1024))
                .setMaxRecordSizeInBytes(Cfg.l("SINK_MAX_ROW_BYTES", 1L * 1024 * 1024))
                .setMaxTimeInBufferMS(Cfg.l("SINK_LINGER_MS", 1_000L))
                .setMaxInFlightRequests(Cfg.i("SINK_MAX_IN_FLIGHT", 2))
                .build();
    }
}
