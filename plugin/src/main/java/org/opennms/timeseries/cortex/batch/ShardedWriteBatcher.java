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
import java.util.concurrent.ExecutionException;
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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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
 *   <li>every series - identified by the exact label set it will carry on the wire, tenant
 *       included - is hashed to exactly one shard, so all writes for a series flow through the
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
 * that goes on. When a request carrying more than one series fails with a plain
 * {@link org.opennms.integration.api.v1.timeseries.StorageException} - a rejection that plausibly
 * names one bad series, such as an out-of-order sample - it is bisected and each half resent (still
 * sequentially, on the shard thread, so ordering holds), cornering a rejected series in O(log n)
 * extra requests rather than one request per series, so a single series the backend rejects does
 * not take unrelated samples down with it. A {@link NonIsolableWriteException} - a rejection of the
 * request itself, such as bad credentials or the wrong tenant, that every series in it would share
 * - is never bisected: every half would fail identically, so isolating it could only spend up to one
 * request per series confirming a foregone conclusion while the shard sat idle and its queue filled
 * up behind it. One retry budget of {@code maxRetries} is shared between a batch and every resend
 * its isolation spawns, so a batch occupies its shard for a bounded number of requests and backoffs
 * even when the backend mixes fatal and retryable failures. A series that still fails, or a request
 * whose budget is exhausted, is dropped and counted on the shared {@code samplesLost} meter, and the
 * shard moves on. That trades bounded head-of-line blocking for forward progress; samples enqueued
 * behind a dropped batch survive.
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

    /**
     * One {@link Series} per distinct series, shared by every {@link Entry} buffered for it,
     * instead of each sample carrying its own copy of the label list and built series key. Weak
     * values: once no buffered or in-flight Entry references a series any more, nothing keeps its
     * entry alive, so this tracks currently-active series rather than growing with every series
     * ever seen over the process lifetime.
     */
    private final Cache<String, Series> seriesCache = CacheBuilder.newBuilder().weakValues().build();

    private final Meter samplesWritten;
    private final Meter samplesLost;
    private final Meter batchesSent;
    private final Meter batchRetries;

    private ShardedWriteBatcher(final Builder builder) {
        this.shardCount = requirePositive(builder.shardCount, "shardCount");
        this.maxBatchSamples = requirePositive(builder.maxBatchSamples, "maxBatchSamples");
        this.lingerMs = requirePositive((int) Math.min(Integer.MAX_VALUE, builder.lingerMs), "lingerMs");
        this.maxRetries = requireNonNegative(builder.maxRetries, "maxRetries");
        this.retryBackoffMs = requirePositive(builder.retryBackoffMs, "retryBackoffMs");
        this.enqueueTimeoutMs = requireNonNegative(builder.enqueueTimeoutMs, "enqueueTimeoutMs");
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

    private static long requirePositive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
        return value;
    }

    private static int requireNonNegative(final int value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative, got " + value);
        }
        return value;
    }

    private static long requireNonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative, got " + value);
        }
        return value;
    }

    /**
     * Queues one sample for delivery. Blocks up to the enqueue timeout when the sample's shard is
     * full (backpressure toward the OpenNMS ring buffer, where drops are already accounted for).
     * Interruption counts the sample as lost and restores the interrupt flag.
     *
     * @return true when the sample was accepted; false when the shard stayed full for the whole
     *         timeout, the caller was interrupted, or the batcher is shut down, in which case the
     *         sample was counted as lost
     */
    public boolean enqueue(final Sample sample, final String organizationId) {
        try {
            return enqueue(sample, organizationId, enqueueTimeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            samplesLost.mark();
            return false;
        }
    }

    /**
     * As {@link #enqueue(Sample, String)}, but with an explicit wait bound, so a caller handing
     * over a whole {@code store()} batch can spread one deadline across it instead of paying the
     * full enqueue timeout once per sample. A non-positive timeout still accepts the sample when
     * the shard has room, it just never waits for it.
     *
     * <p>Interruption propagates without counting the sample as lost: the caller still holds the
     * sample and owns its accounting, and typically wants to stop offering the rest of its batch
     * rather than burn through it against an interrupt flag that fails every offer instantly.
     *
     * @return true when the sample was accepted; false when the shard stayed full for the whole
     *         timeout, the sample could not be converted, or the batcher is shut down, in which
     *         case the sample was counted as lost
     */
    public boolean enqueue(final Sample sample, final String organizationId, final long timeoutMs)
            throws InterruptedException {
        if (!running.get()) {
            samplesLost.mark();
            return false;
        }
        final PrometheusTypes.TimeSeries.Builder converted;
        try {
            converted = seriesConverter.apply(sample);
        } catch (RuntimeException e) {
            // A sample the converter cannot handle must not reach the shard thread, where the
            // same exception would be fatal to every series behind it.
            samplesLost.mark();
            LOG.warn("A sample of metric {} could not be converted to a Prometheus series and is lost.",
                    sample.getMetric(), e);
            return false;
        }
        final Series series = seriesOf(organizationId, converted.getLabelsList());
        final Entry entry = new Entry(sample, series);
        final BlockingQueue<Entry> queue = queues.get(shardOf(series));
        if (queue.offer(entry, Math.max(0, timeoutMs), TimeUnit.MILLISECONDS)) {
            return true;
        }
        samplesLost.mark();
        return false;
    }

    /**
     * Looks up, or creates, the shared {@link Series} for one sample's label set, so every buffered
     * sample of a series points at one copy of its labels and key instead of carrying its own - see
     * {@link #seriesCache}.
     */
    private Series seriesOf(final String organizationId, final List<PrometheusTypes.Label> labels) {
        final String orgKey = organizationId == null ? "" : organizationId;
        final String key = Series.buildSeriesKey(orgKey, labels);
        try {
            return seriesCache.get(key, () -> new Series(organizationId, orgKey, labels, key));
        } catch (ExecutionException e) {
            // The Callable above only allocates; it cannot throw a checked exception.
            throw new IllegalStateException("Unexpected failure resolving a series", e);
        }
    }

    /**
     * The shard hash must be stable for the lifetime of the process; per-series ordering only holds
     * while a series maps to a single shard. Label sets and String.hashCode are both stable, and a
     * restart is safe because no batches are in flight across it.
     */
    private int shardOf(final Series series) {
        return Math.floorMod(series.seriesKey.hashCode(), shardCount);
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
                    flushSafely(batch);
                }
                return;
            }
            flushSafely(batch);
        }
    }

    /**
     * Nothing {@link #flush(List)} throws may take the shard thread down: a dead shard silently
     * strands every series hashed to it until restart, while the other shards look healthy. Drop
     * the batch instead and keep serving.
     */
    private void flushSafely(final List<Entry> batch) {
        try {
            flush(batch);
        } catch (RuntimeException e) {
            drop(batch.size(), "an unexpected error while assembling or sending it", e);
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
                organizationId = series.get(0).organizationId();
                // Entries grouped here share one series key, and the key is derived from the label
                // set, so any entry's labels describe the whole group.
                final PrometheusTypes.TimeSeries.Builder ts = PrometheusTypes.TimeSeries.newBuilder()
                        .addAllLabels(series.get(0).labels());
                for (Entry entry : series) {
                    ts.addSamples(PrometheusTypes.Sample.newBuilder()
                            .setTimestamp(entry.sample.getTime().toEpochMilli())
                            .setValue(entry.sample.getValue()));
                    sampleCount++;
                }
                request.addTimeseries(ts);
            }
            sendWithRetry(request.build(), organizationId, sampleCount, new RetryBudget(maxRetries));
        }
    }

    private void sendWithRetry(final PrometheusRemote.WriteRequest request, final String organizationId,
                               final int sampleCount, final RetryBudget budget) {
        for (int attempt = 0; ; attempt++) {
            try {
                sender.send(request, organizationId);
                batchesSent.mark();
                samplesWritten.mark(sampleCount);
                return;
            } catch (RetryableWriteException e) {
                // The budget is shared with every resend spawned by isolating this request's
                // original batch, so a batch cannot occupy its shard for more than maxRetries
                // backoffs in total, however far it is split.
                if (!budget.tryConsume()) {
                    drop(sampleCount, "the batch's shared budget of " + maxRetries + " retries is exhausted", e);
                    return;
                }
                batchRetries.mark();
                long backoff = retryBackoffMs << Math.min(attempt, 10);
                if (backoff <= 0 || backoff > MAX_BACKOFF_MS) {
                    backoff = MAX_BACKOFF_MS; // the shift overflowed, or the configured base is huge
                }
                LOG.debug("Retryable failure writing a batch of {} samples, attempt {} of this request, "
                                + "backing off {}ms", sampleCount, attempt + 1, backoff, e);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    drop(sampleCount, "interrupted during retry backoff", e);
                    return;
                }
            } catch (NonIsolableWriteException e) {
                // The rejection applies to the whole request, not to any one series in it - see
                // NonIsolableWriteException. Bisecting would only repeat the same failure at every
                // leaf, for up to one request per series, while this shard sits idle and its queue
                // backs up behind it. Drop the whole thing in one step instead.
                drop(sampleCount, "the backend rejected the whole request, not a specific series", e);
                return;
            } catch (StorageException e) {
                final int seriesCount = request.getTimeseriesCount();
                if (seriesCount > 1) {
                    // A non-retryable rejection names one offender at best, but this request
                    // coalesces many unrelated series. Bisect and resend each half - still
                    // sequentially, on this thread, so ordering holds. A healthy half is
                    // confirmed with one request, so a rejected series is cornered in O(log n)
                    // extra requests instead of one request per series of the batch.
                    LOG.warn("The backend rejected a coalesced batch of {} series ({} samples); "
                                    + "bisecting to isolate the rejected series.",
                            seriesCount, sampleCount, e);
                    final int mid = seriesCount / 2;
                    final PrometheusRemote.WriteRequest head = sliceSeries(request, 0, mid);
                    final PrometheusRemote.WriteRequest tail = sliceSeries(request, mid, seriesCount);
                    sendWithRetry(head, organizationId, sampleCountOf(head), budget);
                    sendWithRetry(tail, organizationId, sampleCountOf(tail), budget);
                    return;
                }
                drop(sampleCount, "the backend rejected the batch", e);
                return;
            }
        }
    }

    private static PrometheusRemote.WriteRequest sliceSeries(final PrometheusRemote.WriteRequest request,
                                                             final int from, final int to) {
        final PrometheusRemote.WriteRequest.Builder slice = PrometheusRemote.WriteRequest.newBuilder();
        for (int i = from; i < to; i++) {
            slice.addTimeseries(request.getTimeseries(i));
        }
        return slice.build();
    }

    private static int sampleCountOf(final PrometheusRemote.WriteRequest request) {
        int count = 0;
        for (PrometheusTypes.TimeSeries ts : request.getTimeseriesList()) {
            count += ts.getSamplesCount();
        }
        return count;
    }

    /**
     * Retries left for one original batch and everything its fatal-rejection isolation resends.
     * Only touched from the owning shard thread, so a plain int suffices.
     */
    private static final class RetryBudget {
        private int remaining;

        RetryBudget(final int remaining) {
            this.remaining = remaining;
        }

        boolean tryConsume() {
            if (remaining <= 0) {
                return false;
            }
            remaining--;
            return true;
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

    /** One buffered sample plus a reference to its series. See {@link Series} for why the two are split. */
    private static final class Entry {
        private final Sample sample;
        private final Series series;

        Entry(final Sample sample, final Series series) {
            this.sample = Objects.requireNonNull(sample);
            this.series = Objects.requireNonNull(series);
        }

        String orgKey() {
            return series.orgKey;
        }

        String organizationId() {
            return series.organizationId;
        }

        String seriesKey() {
            return series.seriesKey;
        }

        List<PrometheusTypes.Label> labels() {
            return series.labels;
        }
    }

    /**
     * Identity of one series: the exact label set it will carry on the wire, plus the tenant, since
     * two tenants may legitimately carry the same series. Nothing upstream of the converter can
     * stand in for this. Metric.getKey() covers only the intrinsic tags while the converter also
     * emits the meta tags as labels, so two samples whose meta tags differ are different wire series
     * and must not coalesce; and sanitization is lossy, so two distinct raw keys can emit one and
     * the same label set and must land on the same shard for per-series ordering to hold. The label
     * list arrives sorted by name from the converter, so the key is deterministic.
     *
     * <p>{@link #seriesCache} holds one of these per distinct series and every buffered
     * {@link Entry} for that series points at it, rather than each carrying its own copy of the
     * label list and built key: at default sizing a shard can buffer tens of thousands of samples,
     * almost always many samples per series, so per-sample duplication of series-level data is pure
     * waste - and heaviest exactly when a shard is backlogged, which is when the extra heap and GC
     * pressure can least be afforded.
     */
    private static final class Series {
        private final String organizationId;
        private final String orgKey;
        private final List<PrometheusTypes.Label> labels;
        private final String seriesKey;

        Series(final String organizationId, final String orgKey, final List<PrometheusTypes.Label> labels,
               final String seriesKey) {
            this.organizationId = organizationId;
            this.orgKey = orgKey;
            this.labels = labels;
            this.seriesKey = seriesKey;
        }

        private static String buildSeriesKey(final String orgKey, final List<PrometheusTypes.Label> labels) {
            final StringBuilder key = new StringBuilder(orgKey);
            for (PrometheusTypes.Label label : labels) {
                key.append('\0').append(label.getName()).append('\1').append(label.getValue());
            }
            return key.toString();
        }
    }
}
