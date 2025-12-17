package com.inductiveautomation.ignition.examples.historian.model;

import com.inductiveautomation.historian.gateway.api.query.HistoricalNode;
import com.inductiveautomation.ignition.common.QualifiedPath;
import com.inductiveautomation.ignition.common.sqltags.model.types.DataType;
import org.duckdb.DuckDBColumnType;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNullElse;
import static java.util.Objects.requireNonNullElseGet;

public record DuckDBHistoricalNode(
            UUID nodeId,
            QualifiedPath source,
            Optional<DataType> dataType,
            Optional<DuckDBColumnType> columnType,
            Instant createdTime,
            Optional<Instant> retiredTime) implements Comparable<DuckDBHistoricalNode>, HistoricalNode {

    private static final UUID IMPLICIT = new UUID(0, 0);

    public DuckDBHistoricalNode {
        nodeId = requireNonNullElseGet(nodeId, UUID::randomUUID);
        source = requireNonNullElseGet(source, QualifiedPath::new);
        dataType = requireNonNullElseGet(dataType, Optional::empty);
        columnType = requireNonNullElseGet(columnType, Optional::empty);
        createdTime = requireNonNullElseGet(createdTime, Instant::now);
        retiredTime = requireNonNullElseGet(retiredTime, Optional::empty);
    }

    public static DuckDBHistoricalNode createImplicitNode(QualifiedPath source) {
        return new DuckDBHistoricalNode(IMPLICIT, Objects.requireNonNullElseGet(source,QualifiedPath::new), Optional.empty(), Optional.empty(), Instant.now(), Optional.empty());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(DuckDBHistoricalNode copy) {
        return new Builder(copy);
    }
    
    public boolean isImplicit() {
        return nodeId.equals(IMPLICIT);
    }
    
    public boolean isActiveAt(Instant time) {
        return createdTime.isBefore(time) && retiredTime.isEmpty() || retiredTime.isPresent() && retiredTime.get().isAfter(time);
    }
    
    public boolean isActive() {
        return retiredTime.isEmpty();
    }
    @Override
    public boolean equals(Object o) {
        if(!(o instanceof DuckDBHistoricalNode that)) return false;
        return Objects.equals(nodeId,that.nodeId) && Objects.equals(source,that.source) && Objects.equals(dataType,that.dataType) && Objects.equals(columnType,that.columnType) && Objects.equals(createdTime,that.createdTime) && Objects.equals(retiredTime,that.retiredTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, source, dataType, columnType, createdTime, retiredTime);
    }
    @Override
    public int compareTo(DuckDBHistoricalNode o) {
        return createdTime().compareTo(o.createdTime());
    }

    public static class Builder {
        private UUID nodeId;
        private QualifiedPath source;
        private DataType dataType;
        private DuckDBColumnType columnType;
        private Instant createdTime;
        private Instant retiredTime;

        public Builder() {}

        public Builder(DuckDBHistoricalNode copy) {
            this.nodeId = copy.nodeId;
            this.source = copy.source;
            this.dataType = copy.dataType.orElse(null);
            this.columnType = copy.columnType.orElse(null);
            this.createdTime = copy.createdTime;
            this.retiredTime = copy.retiredTime.orElse(null);
        }

        public Builder nodeId(UUID nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder source(QualifiedPath source) {
            this.source = source;
            return this;
        }

        public Builder dataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder columnType(DuckDBColumnType columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder createdTime(Instant createdTime) {
            this.createdTime = createdTime;
            return this;
        }

        public Builder retiredTime(Instant retiredTime) {
            this.retiredTime = retiredTime;
            return this;
        }

        public DuckDBHistoricalNode build() {
            return new DuckDBHistoricalNode(
                    requireNonNullElse(nodeId, UUID.randomUUID()),
                    requireNonNullElse(source, new QualifiedPath()),
                    Optional.ofNullable(dataType),
                    Optional.ofNullable(columnType), 
                    Objects.requireNonNull(createdTime, "createdTime cannot be null"), 
                    Optional.ofNullable(retiredTime));
        }
    }

}
