package dev.kainth.spatebench.flink;

import java.time.Instant;
import java.util.List;

/**
 * One output row. Field order mirrors {@code sensor_events} in
 * {@code workload/clickhouse/ddl.sql}, which is the wire contract.
 *
 * <p>A plain mutable POJO with public fields and a public no-arg constructor, so
 * Flink resolves the outer class as a POJO, every field with a native
 * serialiser — {@code config.yaml}'s {@code pipeline.generic-types: false}
 * refuses the job at submission otherwise. A chained, object-reusing pipeline
 * avoids inter-operator copies regardless; sink and checkpoint encoding remain.
 *
 * <p>{@link FlattenEvents} re-uses a single instance across the fan-out, which is
 * safe with reference passing because (a) {@code pipeline.object-reuse} is on and the chain hands
 * the reference straight to the sink writer, and (b) the sink's
 * {@code ClickHouseConvertor} copies every field into its own payload map and
 * serialises it before returning. Every value stored here is immutable
 * ({@code String}, boxed primitives, {@code Instant}) or freshly allocated
 * ({@code tags}), so no buffered payload can observe a later mutation.
 */
public final class SensorRow {

    public long batchId;
    public int eventSeq;
    public String sensor;
    public String region;
    public String nameUpper;
    public String unit;
    public long value;
    public long valueScaled;
    /** Nullable(Float64): null must survive as null. */
    public Double quality;
    public List<String> tags;
    public Instant batchTs;
    public Instant sendTs;

    public SensorRow() {}
}
