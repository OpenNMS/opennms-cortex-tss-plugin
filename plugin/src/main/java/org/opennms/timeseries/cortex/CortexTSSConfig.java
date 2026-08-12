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
    private final long externalTagsCacheSize;
    private final long bulkheadMaxWaitDurationInMs;
    private final long maxSeriesLookback;
    private final String organizationId;
    private final boolean hasOrganizationId;
    private final long callTimeoutInMs;
    private final boolean asyncWrites;

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
        this.externalTagsCacheSize = builder.externalTagsCacheSize;
        this.bulkheadMaxWaitDurationInMs = builder.bulkheadMaxWaitDurationInMs;
        this.maxSeriesLookback = builder.maxSeriesLookback;
        this.organizationId = builder.organizationId;
        this.hasOrganizationId = organizationId != null && organizationId.trim().length() > 0;
        this.callTimeoutInMs = builder.callTimeoutInMs;
        this.asyncWrites = builder.asyncWrites;
    }

    /** Will be called via blueprint. The builder can be called when not running as Osgi plugin. */
    public CortexTSSConfig(
            final String writeUrl,
            final String readUrl,
            final int maxConcurrentHttpConnections,
            final long writeTimeoutInMs,
            final long readTimeoutInMs,
            final long metricCacheSize,
            final long externalTagsCacheSize,
            final long bulkheadMaxWaitDurationInMs,
            final long maxSeriesLookback,
            final String organizationId,
            final long callTimeoutInMs,
            final boolean asyncWrites) {
        this(builder()
                .writeUrl(writeUrl)
                .readUrl(readUrl)
                .maxConcurrentHttpConnections(maxConcurrentHttpConnections)
                .writeTimeoutInMs(writeTimeoutInMs)
                .readTimeoutInMs(readTimeoutInMs)
                .metricCacheSize(metricCacheSize)
                .externalCacheSize(externalTagsCacheSize)
                .bulkheadMaxWaitDurationInMs(bulkheadMaxWaitDurationInMs)
                .maxSeriesLookback(maxSeriesLookback)
                .organizationId(organizationId)
                .callTimeoutInMs(callTimeoutInMs)
                .asyncWrites(asyncWrites));
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

    public long getExternalTagsCacheSize() { return externalTagsCacheSize; }

    public long getBulkheadMaxWaitDurationInMs() {
        return bulkheadMaxWaitDurationInMs;
    }

    public long getMaxSeriesLookback() {
        return maxSeriesLookback;
    }

    public boolean hasOrganizationId() {
        return hasOrganizationId;
    }

    public String getOrganizationId() {
        return organizationId;
    }

    /**
     * Budget for a whole remote write call: connect, write, backend processing and reading the ack.
     * Distinct from {@link #getWriteTimeoutInMs()}, which only bounds writing the request body.
     */
    public long getCallTimeoutInMs() {
        return callTimeoutInMs;
    }

    /**
     * When true, {@code store()} dispatches the write and returns without waiting for it, and
     * failures are logged rather than reported to the caller. This is the pre-2.2.0 behaviour, kept
     * as an escape hatch for deployments where blocking the OpenNMS writer threads is worse than
     * losing the error.
     */
    public boolean isAsyncWrites() {
        return asyncWrites;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {
        private String writeUrl = "http://localhost:9009/api/prom/push";
        private String readUrl = "http://localhost:9009/prometheus/api/v1";
        private int maxConcurrentHttpConnections = 100;
        private long writeTimeoutInMs = 5000;
        private long readTimeoutInMs = 5000;
        private long metricCacheSize = 1000;
        private long externalTagsCacheSize = 1000;
        private long bulkheadMaxWaitDurationInMs = Long.MAX_VALUE;
        private long maxSeriesLookback = 7776000;
        private String organizationId = null;
        private long callTimeoutInMs = 10000;
        private boolean asyncWrites = false;

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

        public Builder externalCacheSize(final long externalTagsCacheSize) {
            this.externalTagsCacheSize = externalTagsCacheSize;
            return this;
        }

        public Builder bulkheadMaxWaitDurationInMs(final long bulkheadMaxWaitDurationInMs) {
            this.bulkheadMaxWaitDurationInMs = bulkheadMaxWaitDurationInMs;
            return this;
        }

        public Builder maxSeriesLookback (final long maxSeriesLookback) {
            this.maxSeriesLookback = maxSeriesLookback;
            return this;
        }
        public Builder organizationId(final String organizationId) {
            this.organizationId = organizationId;
            return this;
        }

        public Builder callTimeoutInMs(final long callTimeoutInMs) {
            this.callTimeoutInMs = callTimeoutInMs;
            return this;
        }

        public Builder asyncWrites(final boolean asyncWrites) {
            this.asyncWrites = asyncWrites;
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
                .add("externalCacheSize=" + externalTagsCacheSize)
                .add("bulkheadMaxWaitDurationInMs=" + bulkheadMaxWaitDurationInMs)
                .add("maxSeriesLookback=" + maxSeriesLookback)
                .add("organizationId=" + organizationId)
                .add("callTimeoutInMs=" + callTimeoutInMs)
                .add("asyncWrites=" + asyncWrites)
                .toString();
    }
}
