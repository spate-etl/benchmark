package dev.kainth.spatebench.flink;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.apache.flink.connector.clickhouse.convertor.ColumnBinding;
import org.apache.flink.connector.clickhouse.convertor.DataMapper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * The column contract, as the ClickHouse connector's own {@code DataMapper}
 * declares it.
 *
 * <p>{@link #bindings()} order is the write order within each row, and it mirrors
 * {@code sensor_events} in {@code workload/clickhouse/ddl.sql} exactly. The
 * connector runs typed (POJO) mode, so the wire format is
 * {@code RowBinaryWithNamesAndTypes} and these type expressions are also what
 * goes into the format header — a mismatch with the table is rejected by the
 * server rather than silently coerced.
 *
 * <p>Note the column is {@code name_upper}, not {@code name} — the workload
 * replaces the raw metric name with its ASCII uppercase.
 *
 * <p>{@code ingest_ts} is deliberately absent: it is {@code MATERIALIZED now64(6)}
 * and is the single server-side definition of "arrived" for every arm.
 */
public final class SensorRowMapper extends DataMapper<SensorRow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void toMap(SensorRow r, Map<String, Object> m) {
        // The boxes here are the connector's contract, not our choice: the payload
        // it checkpoints is a Map<String,Object>, and DataWriter dispatches on the
        // declared ClickHouse type with a cast — UInt64 requires a Long, UInt16 a
        // Number, Nullable(Float64) a Double or null.
        m.put("batch_id", r.batchId);
        m.put("event_seq", r.eventSeq);
        m.put("sensor", r.sensor);
        m.put("region", r.region);
        m.put("name_upper", r.nameUpper);
        m.put("unit", r.unit);
        m.put("value", r.value);
        m.put("value_scaled", r.valueScaled);
        m.put("quality", r.quality);
        m.put("tags", r.tags);
        // DataWriter.writeDateTime64 accepts only LocalDateTime/ZonedDateTime.
        m.put("batch_ts", LocalDateTime.ofInstant(r.batchTs, ZoneOffset.UTC));
        m.put("send_ts", LocalDateTime.ofInstant(r.sendTs, ZoneOffset.UTC));
    }

    @Override
    public List<ColumnBinding> bindings() {
        return List.of(
                ColumnBinding.scalar("batch_id", "batch_id", ClickHouseDataType.UInt64),
                ColumnBinding.scalar("event_seq", "event_seq", ClickHouseDataType.UInt16),
                lowCardinality("sensor"),
                lowCardinality("region"),
                lowCardinality("name_upper"),
                lowCardinality("unit"),
                ColumnBinding.scalar("value", "value", ClickHouseDataType.Int64),
                ColumnBinding.scalar("value_scaled", "value_scaled", ClickHouseDataType.Int64),
                ColumnBinding.scalar("quality", "quality", ClickHouseDataType.Float64, true, false),
                ColumnBinding.array("tags", "tags",
                        ClickHouseColumn.of("tags", "LowCardinality(String)")),
                ColumnBinding.dateTime64("batch_ts", "batch_ts", 3),
                ColumnBinding.dateTime64("send_ts", "send_ts", 6));
    }

    static ColumnBinding lowCardinality(String column) {
        return ColumnBinding.scalar(column, column, ClickHouseDataType.String, false, true);
    }
}
