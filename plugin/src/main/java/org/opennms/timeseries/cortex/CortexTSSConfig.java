package org.opennms.timeseries.cortex;

import java.util.Objects;
import java.util.StringJoiner;

public class CortexTSSConfig {
    private final String writeUrl;
    private final String readUrl;
    private final int maxConcurrentHttpConnections;
    private final long writeTimeoutInMs;
    private final long readTimeoutInMs;
    private final long metricCacheSize;
    private final long bulkheadMaxWaitDurationInMs;


    public CortexTSSConfig() {
        this(builder());
    }

    public CortexTSSConfig(Builder builder) {
        this.writeUrl = Objects.requireNonNull(builder.writeUrl);
        this.readUrl = Objects.requireNonNull(builder.readUrl);
        this.maxConcurrentHttpConnections = builder.maxConcurrentHttpConnections;
        this.writeTimeoutInMs = builder.writeTimeoutInMs;
        this.readTimeoutInMs = builder.readTimeoutInMs;
        this.metricCacheSize = builder.metricCacheSize;
        this.bulkheadMaxWaitDurationInMs = builder.bulkheadMaxWaitDurationInMs;
    }

    /** Will be called via blueprint. The builder can be called when not running as Osgi plugin. */
    public CortexTSSConfig(
            final String writeUrl,
            final String readUrl,
            final int maxConcurrentHttpConnections,
            final long writeTimeoutInMs,
            final long readTimeoutInMs,
            final long metricCacheSize,
            final long bulkheadMaxWaitDurationInMs) {
        this(builder()
                .writeUrl(writeUrl)
                .readUrl(readUrl)
                .maxConcurrentHttpConnections(maxConcurrentHttpConnections)
                .writeTimeoutInMs(writeTimeoutInMs)
                .readTimeoutInMs(readTimeoutInMs)
        .metricCacheSize(metricCacheSize)
        .bulkheadMaxWaitDurationInMs(bulkheadMaxWaitDurationInMs));
    }

    public String getWriteUrl() {
        return writeUrl;
    }

    public String getReadUrl() {
        return readUrl;
    }

    public int getMaxConcurrentHttpConnections() {
        return maxConcurrentHttpConnections;
    }

    public long getWriteTimeoutInMs() {
        return writeTimeoutInMs;
    }

    public long getReadTimeoutInMs() {
        return readTimeoutInMs;
    }

    public long getMetricCacheSize() {
        return metricCacheSize;
    }

    public long getBulkheadMaxWaitDurationInMs() {
        return bulkheadMaxWaitDurationInMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {
        private String writeUrl = "http://localhost:9009/api/prom/push";
        private String readUrl = "http://localhost:9009/prometheus/api/v1";
        private int maxConcurrentHttpConnections = 100;
        private long writeTimeoutInMs = 1000;
        private long readTimeoutInMs = 1000;
        private long metricCacheSize = 1000;
        private long bulkheadMaxWaitDurationInMs = Long.MAX_VALUE;

        public Builder writeUrl(final String writeUrl) {
            this.writeUrl = writeUrl;
            return this;
        }

        public Builder readUrl(final String readUrl) {
            this.readUrl = readUrl;
            return this;
        }

        public Builder maxConcurrentHttpConnections(final int maxConcurrentHttpConnections) {
            this.maxConcurrentHttpConnections = maxConcurrentHttpConnections;
            return this;
        }

        public Builder writeTimeoutInMs(final long writeTimeoutInMs) {
            this.writeTimeoutInMs = writeTimeoutInMs;
            return this;
        }

        public Builder readTimeoutInMs(final long readTimeoutInMs) {
            this.readTimeoutInMs = readTimeoutInMs;
            return this;
        }

        public Builder metricCacheSize(final long metricCacheSize) {
            this.metricCacheSize = metricCacheSize;
            return this;
        }

        public Builder bulkheadMaxWaitDurationInMs(final long bulkheadMaxWaitDurationInMs) {
            this.bulkheadMaxWaitDurationInMs = bulkheadMaxWaitDurationInMs;
            return this;
        }

        public CortexTSSConfig build() {
            return new CortexTSSConfig(this);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CortexTSSConfig.class.getSimpleName() + "[", "]")
                .add("writeUrl='" + writeUrl + "'")
                .add("readUrl='" + readUrl + "'")
                .add("maxConcurrentHttpConnections=" + maxConcurrentHttpConnections)
                .add("writeTimeoutInMs=" + writeTimeoutInMs)
                .add("readTimeoutInMs=" + readTimeoutInMs)
                .add("metricCacheSize=" + metricCacheSize)
                .add("bulkheadMaxWaitDurationInMs=" + bulkheadMaxWaitDurationInMs)
                .toString();
    }
}
