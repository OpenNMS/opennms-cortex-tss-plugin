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
package org.opennms.timeseries.cortex.batch;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

/**
 * Coalesces the small per-group sample lists that OpenNMS hands to {@code store()} into large
 * remote-write requests, while honoring the Prometheus remote-write ordering rule: samples must be
 * in timestamp order per series, and requests may only run in parallel when they carry disjoint
 * series (https://prometheus.io/docs/specs/prw/remote_write_spec/).
 *
 * <p>Ordering is a structural invariant here, not a scheduling accident:
 * <ul>
 *   <li>every series is hashed to exactly one shard, so all writes for a series flow through the
 *       same shard forever;</li>
 *   <li>each shard flushes from a single thread and keeps at most one request in flight, retries
 *       included, so a shard's batches reach the backend in the order they were assembled;</li>
 *   <li>within a batch, samples are grouped into one TimeSeries entry per series and sorted by
 *       timestamp.</li>
 * </ul>
 * Different shards flush in parallel, which the spec allows because their series sets are disjoint.
 *
 * <p>A batch is flushed when it reaches {@code maxBatchSamples} or when {@code lingerMs} elapses
 * after its first sample, whichever comes first (the same shape as Prometheus's own
 * {@code max_samples_per_send} / {@code batch_send_deadline} queue configuration).
 *
 * <p>Failure semantics: a batch that fails with a {@link RetryableWriteException} is retried in
 * place with exponential backoff up to {@code maxRetries} times; the shard sends nothing else while
 * that goes on. A batch that fails fatally, or exhausts its retries, is dropped and counted on the
 * shared {@code samplesLost} meter, and the shard moves on. That trades bounded head-of-line
 * blocking for forward progress; samples enqueued behind a dropped batch survive.
 */
public class ShardedWriteBatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ShardedWriteBatcher.class);

    /** How long an idle shard thread waits for work before re-checking the shutdown flag. */
    private static final long IDLE_POLL_MS = 100;

    /** Upper bound for the exponential retry backoff. */
    private static final long MAX_BACKOFF_MS = 60_000;

    /** Grace period for draining all shards on {@link #destroy()}. */
    private static final long DESTROY_GRACE_MS = 30_000;

    private final int shardCount;
    private final int maxBatchSamples;
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long enqueueTimeoutMs;

    private final Function<Sample, PrometheusTypes.TimeSeries.Builder> seriesConverter;
    private final RemoteWriteSender sender;

    private final List<BlockingQueue<Entry>> queues;
    private final List<Thread> shardThreads = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final Meter samplesWritten;
    private final Meter samplesLost;
    private final Meter batchesSent;
    private final Meter batchRetries;

    private ShardedWriteBatcher(final Builder builder) {
        this.shardCount = requirePositive(builder.shardCount, "shardCount");
        this.maxBatchSamples = requirePositive(builder.maxBatchSamples, "maxBatchSamples");
        this.lingerMs = requirePositive((int) Math.min(Integer.MAX_VALUE, builder.lingerMs), "lingerMs");
        this.maxRetries = builder.maxRetries;
        this.retryBackoffMs = builder.retryBackoffMs;
        this.enqueueTimeoutMs = builder.enqueueTimeoutMs;
        this.seriesConverter = Objects.requireNonNull(builder.seriesConverter, "seriesConverter");
        this.sender = Objects.requireNonNull(builder.sender, "sender");

        final MetricRegistry registry = Objects.requireNonNull(builder.metricRegistry, "metricRegistry");
        // Same meter names CortexTSS already exposes, so batched writes show up in the existing
        // opennms-cortex:stats output rather than in a parallel set of counters.
        this.samplesWritten = registry.meter("samplesWritten");
        this.samplesLost = registry.meter("samplesLost");
        this.batchesSent = registry.meter("batch.batchesSent");
        this.batchRetries = registry.meter("batch.retries");

        final int shardCapacity = requirePositive(builder.shardCapacity, "shardCapacity");
        this.queues = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            queues.add(new LinkedBlockingQueue<>(shardCapacity));
        }
        registry.register("batch.bufferedSamples",
                (Gauge<Integer>) () -> queues.stream().mapToInt(BlockingQueue::size).sum());

        for (int i = 0; i < shardCount; i++) {
            final int shard = i;
            final Thread thread = new Thread(() -> runShard(shard), "PrometheusWriteShard-" + i);
            thread.setDaemon(true);
            shardThreads.add(thread);
            thread.start();
        }

        LOG.info("Started sharded write batcher: shards={}, maxBatchSamples={}, lingerMs={}, "
                        + "shardCapacity={}, maxRetries={}, retryBackoffMs={}",
                shardCount, maxBatchSamples, lingerMs, shardCapacity, maxRetries, retryBackoffMs);
    }

    private static int requirePositive(final int value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
        return value;
    }

    /**
     * Queues one sample for delivery. Blocks up to the enqueue timeout when the sample's shard is
     * full (backpressure toward the OpenNMS ring buffer, where drops are already accounted for).
     *
     * @return true when the sample was accepted; false when the shard stayed full for the whole
     *         timeout or the batcher is shut down, in which case the sample was counted as lost
     */
    public boolean enqueue(final Sample sample, final String organizationId) {
        if (!running.get()) {
            samplesLost.mark();
            return false;
        }
        final Entry entry = new Entry(sample, organizationId);
        final BlockingQueue<Entry> queue = queues.get(shardOf(entry));
        try {
            if (queue.offer(entry, enqueueTimeoutMs, TimeUnit.MILLISECONDS)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        samplesLost.mark();
        return false;
    }

    /**
     * The shard hash must be stable for the lifetime of the process; per-series ordering only holds
     * while a series maps to a single shard. Metric keys and String.hashCode are both stable, and
     * a restart is safe because no batches are in flight across it.
     */
    private int shardOf(final Entry entry) {
        return Math.floorMod(entry.seriesKey().hashCode(), shardCount);
    }

    private void runShard(final int shard) {
        final BlockingQueue<Entry> queue = queues.get(shard);
        final List<Entry> batch = new ArrayList<>(maxBatchSamples);
        while (running.get() || !queue.isEmpty()) {
            batch.clear();
            try {
                final Entry first = queue.poll(IDLE_POLL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                gather(queue, batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Interruption is the destroy() last resort: flush what we hold, then exit.
                if (!batch.isEmpty()) {
                    flush(batch);
                }
                return;
            }
            flush(batch);
        }
    }

    /** Fills the batch until it is full or the linger deadline passes. Never lingers on shutdown. */
    private void gather(final BlockingQueue<Entry> queue, final List<Entry> batch) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lingerMs);
        while (batch.size() < maxBatchSamples) {
            if (!running.get()) {
                // Draining: take whatever is already buffered, but do not wait for more.
                final Entry entry = queue.poll();
                if (entry == null) {
                    return;
                }
                batch.add(entry);
                continue;
            }
            final long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            // Poll in short slices so a shutdown is noticed within IDLE_POLL_MS rather than after
            // a full linger.
            final long slice = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(IDLE_POLL_MS));
            final Entry entry = queue.poll(slice, TimeUnit.NANOSECONDS);
            if (entry != null) {
                batch.add(entry);
            }
        }
    }

    private void flush(final List<Entry> batch) {
        // One request carries one X-Scope-OrgID header, so split by tenant first, then coalesce
        // each series into a single TimeSeries entry with its samples in timestamp order.
        final Map<String, Map<String, List<Entry>>> byOrg = new LinkedHashMap<>();
        for (Entry entry : batch) {
            byOrg.computeIfAbsent(entry.orgKey(), k -> new LinkedHashMap<>())
                    .computeIfAbsent(entry.seriesKey(), k -> new ArrayList<>())
                    .add(entry);
        }

        for (Map.Entry<String, Map<String, List<Entry>>> org : byOrg.entrySet()) {
            final PrometheusRemote.WriteRequest.Builder request = PrometheusRemote.WriteRequest.newBuilder();
            int sampleCount = 0;
            String organizationId = null;
            for (List<Entry> series : org.getValue().values()) {
                series.sort(Comparator.comparing(e -> e.sample.getTime()));
                organizationId = series.get(0).organizationId;
                final PrometheusTypes.TimeSeries.Builder ts = seriesConverter.apply(series.get(0).sample);
                ts.clearSamples();
                for (Entry entry : series) {
                    ts.addSamples(PrometheusTypes.Sample.newBuilder()
                            .setTimestamp(entry.sample.getTime().toEpochMilli())
                            .setValue(entry.sample.getValue()));
                    sampleCount++;
                }
                request.addTimeseries(ts);
            }
            sendWithRetry(request.build(), organizationId, sampleCount);
        }
    }

    private void sendWithRetry(final PrometheusRemote.WriteRequest request, final String organizationId,
                               final int sampleCount) {
        for (int attempt = 0; ; attempt++) {
            try {
                sender.send(request, organizationId);
                batchesSent.mark();
                samplesWritten.mark(sampleCount);
                return;
            } catch (RetryableWriteException e) {
                if (attempt >= maxRetries) {
                    drop(sampleCount, "retries exhausted after " + (attempt + 1) + " attempts", e);
                    return;
                }
                batchRetries.mark();
                final long backoff = Math.min(MAX_BACKOFF_MS, retryBackoffMs << Math.min(attempt, 10));
                LOG.debug("Retryable failure writing a batch of {} samples, attempt {}/{}, backing off {}ms",
                        sampleCount, attempt + 1, maxRetries + 1, backoff, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    drop(sampleCount, "interrupted during retry backoff", e);
                    return;
                }
            } catch (StorageException e) {
                drop(sampleCount, "the backend rejected the batch", e);
                return;
            }
        }
    }

    private void drop(final int sampleCount, final String reason, final Exception cause) {
        samplesLost.mark(sampleCount);
        LOG.error("Dropping a batch of {} samples: {}", sampleCount, reason, cause);
    }

    /** Stops accepting samples, drains what is buffered, and joins the shard threads. */
    public void destroy() {
        running.set(false);
        final long deadline = System.currentTimeMillis() + DESTROY_GRACE_MS;
        for (Thread thread : shardThreads) {
            try {
                thread.join(Math.max(1, deadline - System.currentTimeMillis()));
                if (thread.isAlive()) {
                    thread.interrupt();
                    thread.join(1_000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        final int stranded = queues.stream().mapToInt(BlockingQueue::size).sum();
        if (stranded > 0) {
            samplesLost.mark(stranded);
            LOG.warn("Shut down with {} samples still buffered; they are lost.", stranded);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int shardCount = 8;
        private int maxBatchSamples = 2000;
        private long lingerMs = 500;
        private int shardCapacity = 65_536;
        private int maxRetries = 3;
        private long retryBackoffMs = 1_000;
        private long enqueueTimeoutMs = 5_000;
        private Function<Sample, PrometheusTypes.TimeSeries.Builder> seriesConverter;
        private RemoteWriteSender sender;
        private MetricRegistry metricRegistry;

        public Builder shardCount(final int shardCount) {
            this.shardCount = shardCount;
            return this;
        }

        public Builder maxBatchSamples(final int maxBatchSamples) {
            this.maxBatchSamples = maxBatchSamples;
            return this;
        }

        public Builder lingerMs(final long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder shardCapacity(final int shardCapacity) {
            this.shardCapacity = shardCapacity;
            return this;
        }

        public Builder maxRetries(final int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoffMs(final long retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder enqueueTimeoutMs(final long enqueueTimeoutMs) {
            this.enqueueTimeoutMs = enqueueTimeoutMs;
            return this;
        }

        public Builder seriesConverter(final Function<Sample, PrometheusTypes.TimeSeries.Builder> seriesConverter) {
            this.seriesConverter = seriesConverter;
            return this;
        }

        public Builder sender(final RemoteWriteSender sender) {
            this.sender = sender;
            return this;
        }

        public Builder metricRegistry(final MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public ShardedWriteBatcher build() {
            return new ShardedWriteBatcher(this);
        }
    }

    private static final class Entry {
        private final Sample sample;
        private final String organizationId;

        Entry(final Sample sample, final String organizationId) {
            this.sample = Objects.requireNonNull(sample);
            this.organizationId = organizationId;
        }

        String orgKey() {
            return organizationId == null ? "" : organizationId;
        }

        /**
         * Identity of the series this sample belongs to. Metric.getKey() is 1:1 with the label set
         * produced by the converter, and two tenants may legitimately carry the same series, so the
         * tenant is part of the identity.
         */
        String seriesKey() {
            return orgKey() + " " + sample.getMetric().getKey();
        }
    }
}
