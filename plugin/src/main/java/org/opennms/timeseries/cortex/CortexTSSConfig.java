/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
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
    private final boolean batchingEnabled;
    private final int batchShards;
    private final int batchMaxSamples;
    private final long batchLingerMs;
    private final int batchShardCapacity;
    private final int batchMaxRetries;
    private final long batchRetryBackoffMs;
    private final long batchEnqueueTimeoutMs;

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
        this.batchingEnabled = builder.batchingEnabled;
        this.batchShards = builder.batchShards;
        this.batchMaxSamples = builder.batchMaxSamples;
        this.batchLingerMs = builder.batchLingerMs;
        this.batchShardCapacity = builder.batchShardCapacity;
        this.batchMaxRetries = builder.batchMaxRetries;
        this.batchRetryBackoffMs = builder.batchRetryBackoffMs;
        this.batchEnqueueTimeoutMs = builder.batchEnqueueTimeoutMs;
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
            final boolean asyncWrites,
            final boolean batchingEnabled,
            final int batchShards,
            final int batchMaxSamples,
            final long batchLingerMs,
            final int batchShardCapacity,
            final int batchMaxRetries,
            final long batchRetryBackoffMs,
            final long batchEnqueueTimeoutMs) {
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
                .asyncWrites(asyncWrites)
                .batchingEnabled(batchingEnabled)
                .batchShards(batchShards)
                .batchMaxSamples(batchMaxSamples)
                .batchLingerMs(batchLingerMs)
                .batchShardCapacity(batchShardCapacity)
                .batchMaxRetries(batchMaxRetries)
                .batchRetryBackoffMs(batchRetryBackoffMs)
                .batchEnqueueTimeoutMs(batchEnqueueTimeoutMs));
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

    /**
     * When true, {@code store()} enqueues samples into a sharded batcher that coalesces them into
     * large remote-write requests with per-series ordering guaranteed structurally. When false,
     * every {@code store()} call becomes one immediate request, as before.
     */
    public boolean isBatchingEnabled() {
        return batchingEnabled;
    }

    /** Number of batcher shards, i.e. the write parallelism. 0 derives a default from maxConcurrentHttpConnections. */
    public int getBatchShards() {
        return batchShards;
    }

    public int getBatchMaxSamples() {
        return batchMaxSamples;
    }

    /** How long a batch may wait for more samples after its first one before it is flushed. */
    public long getBatchLingerMs() {
        return batchLingerMs;
    }

    /** Buffered-sample capacity per shard; a full shard pushes back on the OpenNMS writer threads. */
    public int getBatchShardCapacity() {
        return batchShardCapacity;
    }

    public int getBatchMaxRetries() {
        return batchMaxRetries;
    }

    public long getBatchRetryBackoffMs() {
        return batchRetryBackoffMs;
    }

    /**
     * Upper bound on how long one {@code store()} call may block on full shards before the write
     * fails, shared across all samples of the call rather than paid per sample.
     */
    public long getBatchEnqueueTimeoutMs() {
        return batchEnqueueTimeoutMs;
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
        private boolean batchingEnabled = false;
        private int batchShards = 0;
        private int batchMaxSamples = 2000;
        private long batchLingerMs = 500;
        private int batchShardCapacity = 65536;
        private int batchMaxRetries = 3;
        private long batchRetryBackoffMs = 1000;
        private long batchEnqueueTimeoutMs = 5000;

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

        public Builder batchingEnabled(final boolean batchingEnabled) {
            this.batchingEnabled = batchingEnabled;
            return this;
        }

        public Builder batchShards(final int batchShards) {
            this.batchShards = batchShards;
            return this;
        }

        public Builder batchMaxSamples(final int batchMaxSamples) {
            this.batchMaxSamples = batchMaxSamples;
            return this;
        }

        public Builder batchLingerMs(final long batchLingerMs) {
            this.batchLingerMs = batchLingerMs;
            return this;
        }

        public Builder batchShardCapacity(final int batchShardCapacity) {
            this.batchShardCapacity = batchShardCapacity;
            return this;
        }

        public Builder batchMaxRetries(final int batchMaxRetries) {
            this.batchMaxRetries = batchMaxRetries;
            return this;
        }

        public Builder batchRetryBackoffMs(final long batchRetryBackoffMs) {
            this.batchRetryBackoffMs = batchRetryBackoffMs;
            return this;
        }

        public Builder batchEnqueueTimeoutMs(final long batchEnqueueTimeoutMs) {
            this.batchEnqueueTimeoutMs = batchEnqueueTimeoutMs;
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
                .add("batchingEnabled=" + batchingEnabled)
                .add("batchShards=" + batchShards)
                .add("batchMaxSamples=" + batchMaxSamples)
                .add("batchLingerMs=" + batchLingerMs)
                .add("batchShardCapacity=" + batchShardCapacity)
                .add("batchMaxRetries=" + batchMaxRetries)
                .add("batchRetryBackoffMs=" + batchRetryBackoffMs)
                .add("batchEnqueueTimeoutMs=" + batchEnqueueTimeoutMs)
                .toString();
    }
}
