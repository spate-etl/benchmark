package dev.kainth.spatebench.flink;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.List;

/**
 * The workload's flatten: one {@code SensorBatch} message becomes one row per
 * surviving event, with the specified filters and derivations applied in the
 * specified order.
 *
 * <ol>
 *   <li>drop rows where {@code unit = 'drop'}</li>
 *   <li>drop rows where {@code quality} is non-null and {@code < 0.2}</li>
 *   <li>coalesce a null {@code region} to {@code ""}</li>
 *   <li>{@code value_scaled = value * 1000 / (event_seq + 1)}, integer division</li>
 *   <li>{@code name_upper} = ASCII-only uppercase of {@code name}</li>
 * </ol>
 *
 * <p>The two filters run before any per-row conversion, so a dropped row costs
 * one string compare and one unbox rather than a full row build. That ordering is
 * also what the contract specifies, so it is not a liberty taken for speed.
 */
public final class FlattenEvents extends RichFlatMapFunction<GenericRecord, SensorRow> {

    private static final long serialVersionUID = 1L;

    private final String schemaJson;

    /**
     * Re-used across the fan-out; see {@link SensorRow} for why that is safe.
     * Transient because it is per-subtask state, created in {@link #open}.
     */
    private transient SensorRow row;

    public FlattenEvents(String schemaJson) {
        this.schemaJson = schemaJson;
    }

    @Override
    public void open(OpenContext openContext) {
        SensorBatchSchema.assertFieldOrder(SensorBatchSchema.parse(schemaJson));
        row = new SensorRow();
    }

    @Override
    public void flatMap(GenericRecord batch, Collector<SensorRow> out) {
        long batchId = (Long) batch.get(SensorBatchSchema.BATCH_ID);
        String sensor = batch.get(SensorBatchSchema.SENSOR).toString();
        Object regionRaw = batch.get(SensorBatchSchema.REGION);
        String region = regionRaw == null ? "" : regionRaw.toString();

        // Hoisted out of the event loop: both timestamps are per-message, so one
        // Instant pair is shared by all rows of the batch. Instant is immutable,
        // so sharing it across buffered payloads is safe.
        Instant batchTs =
                SensorBatchSchema.fromEpochMillis((Long) batch.get(SensorBatchSchema.BATCH_TS_MS));
        Instant sendTs =
                SensorBatchSchema.fromEpochMicros((Long) batch.get(SensorBatchSchema.SEND_TS_US));

        List<?> events = (List<?>) batch.get(SensorBatchSchema.EVENTS);
        for (int i = 0, n = events.size(); i < n; i++) {
            GenericRecord ev = (GenericRecord) events.get(i);

            String unit = ev.get(SensorBatchSchema.EV_UNIT).toString();
            if (Rows.DROP_UNIT.equals(unit)) {
                continue;
            }
            Double quality = (Double) ev.get(SensorBatchSchema.EV_QUALITY);
            if (quality != null && quality < Rows.QUALITY_FLOOR) {
                continue;
            }

            int seq = (Integer) ev.get(SensorBatchSchema.EV_SEQ);
            long value = (Long) ev.get(SensorBatchSchema.EV_VALUE);

            SensorRow r = row;
            r.batchId = batchId;
            r.eventSeq = seq;
            r.sensor = sensor;
            r.region = region;
            r.nameUpper = Rows.asciiUpper(ev.get(SensorBatchSchema.EV_NAME).toString());
            r.unit = unit;
            r.value = value;
            r.valueScaled = Rows.valueScaled(value, seq);
            r.quality = quality;
            r.tags = Rows.tags(ev.get(SensorBatchSchema.EV_TAGS));
            r.batchTs = batchTs;
            r.sendTs = sendTs;
            out.collect(r);
        }
    }
}
