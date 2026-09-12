package dev.kainth.spatebench.flink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSinkSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTest {
    @Test
    void generatedSpecificRecordsDecodeTheSameCanonicalDatum() throws Exception {
        var schema = SensorBatchSchema.parse(SensorBatchSchema.json());
        var bytes = new java.io.ByteArrayOutputStream();
        var encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(bytes, null);
        new org.apache.avro.generic.GenericDatumWriter<GenericRecord>(schema).write(batch(schema, 42L, null), encoder);
        encoder.flush();
        org.apache.avro.specific.SpecificData.getForClass(rs.etl.bench.SensorBatch.class).setCustomCoders(true);
        var decoder = org.apache.flink.formats.avro.AvroDeserializationSchema.forSpecific(rs.etl.bench.SensorBatch.class);
        var decoded = decoder.deserialize(bytes.toByteArray());
        assertEquals(42L, decoded.getBatchId());
        assertNull(decoded.getRegion());
        assertEquals("aßız", decoded.getEvents().get(0).getName());
        assertEquals(List.of("tag-a", "tag-b"), decoded.getEvents().get(0).getTags());
        GenericRecord record = decoded;
        assertEquals(42L, record.get(SensorBatchSchema.BATCH_ID));
    }

    @Test
    void equalParallelismProducesOneChainAndTheRowIsAPojo() {
        var type = assertInstanceOf(PojoTypeInfo.class, TypeExtractor.getForClass(SensorRow.class));
        assertEquals(12, type.getArity());
        // A POJO is not itself a promise of no Kryo; config.yaml's
        // pipeline.generic-types: false enforces the same thing at submission.
        for (int i = 0; i < type.getArity(); i++) {
            var field = type.getPojoFieldAt(i);
            assertFalse(field.getTypeInformation() instanceof GenericTypeInfo,
                    field.getField().getName() + " resolved to a generic (Kryo) type");
        }
        for (String mode : new String[] {"generic", "specific"}) {
            for (int parallelism : new int[] {8, 16, 32}) {
                var configuration = new Configuration();
                configuration.set(PipelineOptions.OBJECT_REUSE, true);
                var env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
                env.setParallelism(parallelism);
                ComparisonJob.pipeline(env, ComparisonJob.kafkaSource(SensorBatchSchema.parse(SensorBatchSchema.json()), "earliest", mode),
                        new DiscardingSink<>(), SensorBatchSchema.json(), "sensor_events");
                var graph = env.getStreamGraph().getJobGraph();
                assertEquals(1, graph.getNumberOfVertices());
                assertEquals(parallelism, graph.getVertices().iterator().next().getParallelism());
                assertTrue(env.getConfig().isObjectReuseEnabled());
            }
        }
    }

    @Test
    void shippedRateLimiterStartsWithOneBatchDespiteFourRequestSlots() {
        int batchSize = 262144;
        var configuration = org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.builder()
                .setMaxBatchSize(batchSize).setMaxBatchSizeInBytes(67108864)
                .setMaxInFlightRequests(4).setMaxBufferedRequests(2 * batchSize)
                .setMaxTimeInBufferMS(500).setMaxRecordSizeInBytes(1048576).build();
        var limiter = configuration.getRateLimitingStrategy();
        var request = new org.apache.flink.connector.base.sink.writer.strategy.BasicRequestInfo(batchSize);
        assertEquals(batchSize, limiter.getMaxBatchSize());
        assertFalse(limiter.shouldBlock(request));
        limiter.registerInFlightRequest(request);
        assertTrue(limiter.shouldBlock(request));
        limiter.registerCompletedRequest(new org.apache.flink.connector.base.sink.writer.strategy.BasicResultInfo(0, batchSize));
        assertEquals(batchSize + 10, limiter.getMaxBatchSize());
        limiter.registerInFlightRequest(request);
        assertTrue(limiter.shouldBlock(request));
    }

    @Test
    void reusedRowsSurviveAnotherBatchAndConnectorCheckpointRoundTrip() throws Exception {
        for (boolean specificMode : new boolean[] {false, true}) {
            Schema schema = SensorBatchSchema.parse(SensorBatchSchema.json());
            var converter = new ClickHouseConvertor<>(SensorRow.class, new SensorRowMapper());
            converter.open(null);
            List<ClickHousePayload> payloads = new ArrayList<>();
            Collector<SensorRow> collector = new Collector<>() {
                public void collect(SensorRow row) { payloads.add(converter.apply(row, null)); }
                public void close() {}
            };
            var flatten = new FlattenEvents(schema.toString());
            flatten.open(null);
            flatten.flatMap(specificMode ? specific(batch(schema, 11L, null)) : batch(schema, 11L, null), collector);
            flatten.flatMap(specificMode ? specific(batch(schema, 12L, "next")) : batch(schema, 12L, "next"), collector);
            assertEquals(4, payloads.size());
            Map<String, Object> first = payloads.get(0).getData();
            assertEquals(11L, first.get("batch_id"));
            assertEquals("", first.get("region"));
            assertEquals("AßıZ", first.get("name_upper"));
            assertEquals(-2333L, first.get("value_scaled"));
            assertNull(first.get("quality"));
            assertEquals(List.of("tag-a", "tag-b"), first.get("tags"));
            assertEquals(LocalDateTime.ofInstant(SensorBatchSchema.fromEpochMillis(1700000000123L), ZoneOffset.UTC), first.get("batch_ts"));
            assertEquals(LocalDateTime.ofInstant(SensorBatchSchema.fromEpochMicros(1700000000123456L), ZoneOffset.UTC), first.get("send_ts"));
            assertEquals("next", payloads.get(2).getData().get("region"));
            var wrappers = payloads.stream().map(p -> new RequestEntryWrapper<>(p, p.getCachedBytesLength())).toList();
            var serializer = new ClickHouseAsyncSinkSerializer(false);
            byte[] checkpoint = serializer.serialize(new BufferedRequestState<>(wrappers));
            var restored = serializer.deserialize(serializer.getVersion(), checkpoint).getBufferedRequestEntries();
            assertEquals(payloads.size(), restored.size());
            int i = 0;
            for (var entry : restored) {
                assertEquals(payloads.get(i++).getData(), entry.getRequestEntry().getData());
            }
        }
    }

    private static GenericRecord specific(GenericRecord record) throws Exception {
        var bytes = new java.io.ByteArrayOutputStream();
        var encoder = org.apache.avro.io.EncoderFactory.get().binaryEncoder(bytes, null);
        new org.apache.avro.generic.GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        org.apache.avro.specific.SpecificData.getForClass(rs.etl.bench.SensorBatch.class).setCustomCoders(true);
        return org.apache.flink.formats.avro.AvroDeserializationSchema.forSpecific(rs.etl.bench.SensorBatch.class)
                .deserialize(bytes.toByteArray());
    }

    private static GenericRecord batch(Schema schema, long id, String region) {
        GenericRecord batch = new GenericData.Record(schema);
        batch.put("batch_id", id);
        batch.put("sensor", "sensor-test");
        batch.put("region", region);
        batch.put("batch_ts_ms", 1700000000123L);
        batch.put("send_ts_us", 1700000000123456L);
        Schema eventSchema = schema.getField("events").schema().getElementType();
        List<GenericRecord> events = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            GenericRecord event = new GenericData.Record(eventSchema);
            event.put("seq", i + 2);
            event.put("name", "aßız");
            event.put("unit", i == 1 ? "drop" : "unit");
            event.put("value", -7L);
            event.put("quality", i == 0 ? null : i == 2 ? 0.19 : 0.2);
            event.put("tags", List.of("tag-a", "tag-b"));
            events.add(event);
        }
        batch.put("events", events);
        return batch;
    }
}
