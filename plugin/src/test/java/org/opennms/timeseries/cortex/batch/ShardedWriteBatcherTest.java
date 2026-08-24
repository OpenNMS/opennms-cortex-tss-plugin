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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.timeseries.cortex.CortexTSS;

import com.codahale.metrics.MetricRegistry;

import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

/**
 * Exercises {@link ShardedWriteBatcher} against a recording in-memory sender: batch assembly (size
 * and linger triggers), per-series coalescing and timestamp sorting, per-series ordering across
 * batches, tenant splitting, retry/drop semantics, and drain-on-destroy.
 */
public class ShardedWriteBatcherTest {

    private static final Instant BASE = Instant.parse("2026-08-24T00:00:00Z").truncatedTo(ChronoUnit.SECONDS);

    private final MetricRegistry registry = new MetricRegistry();
    private final RecordingSender sender = new RecordingSender();
    private ShardedWriteBatcher batcher;

    @After
    public void tearDown() {
        if (batcher != null) {
            batcher.destroy();
            batcher = null;
        }
    }

    private ShardedWriteBatcher.Builder builder() {
        return ShardedWriteBatcher.builder()
                .seriesConverter(CortexTSS::toPrometheusTimeSeries)
                .sender(sender)
                .metricRegistry(registry);
    }

    // ------------------------------------------------------------------
    // Batch assembly
    // ------------------------------------------------------------------

    @Test
    public void flushesWhenTheBatchIsFull() {
        batcher = builder().shardCount(1).maxBatchSamples(5).lingerMs(60_000).build();

        Metric metric = gauge("full_batch");
        for (int i = 0; i < 5; i++) {
            assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null));
        }

        // The linger is a minute; only the size trigger can flush this.
        SentBatch sent = sender.awaitNext();
        assertEquals(1, sent.request.getTimeseriesCount());
        assertEquals(5, sent.request.getTimeseries(0).getSamplesCount());
        assertEquals(5, registry.meter("samplesWritten").getCount());
    }

    @Test
    public void flushesOnLingerWhenTheBatchIsNotFull() {
        batcher = builder().shardCount(1).maxBatchSamples(1000).lingerMs(100).build();

        Metric metric = gauge("linger_batch");
        for (int i = 0; i < 3; i++) {
            assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null));
        }

        SentBatch sent = sender.awaitNext();
        assertEquals(3, sent.request.getTimeseries(0).getSamplesCount());
    }

    @Test
    public void coalescesSeriesAndSortsSamplesByTimestamp() {
        batcher = builder().shardCount(1).maxBatchSamples(6).lingerMs(60_000).build();

        Metric a = gauge("series_a");
        Metric b = gauge("series_b");
        // Interleaved series, timestamps deliberately out of order per series.
        batcher.enqueue(sample(a, BASE.plusSeconds(2), 1), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(1), 2), null);
        batcher.enqueue(sample(a, BASE.plusSeconds(0), 3), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(3), 4), null);
        batcher.enqueue(sample(a, BASE.plusSeconds(1), 5), null);
        batcher.enqueue(sample(b, BASE.plusSeconds(2), 6), null);

        SentBatch sent = sender.awaitNext();
        assertEquals(2, sent.request.getTimeseriesCount());
        for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
            assertEquals(3, ts.getSamplesCount());
            assertAscending(ts);
        }
    }

    @Test
    public void splitsTenantsIntoSeparateRequests() {
        batcher = builder().shardCount(1).maxBatchSamples(1000).lingerMs(100).build();

        Metric metric = gauge("multi_tenant");
        batcher.enqueue(sample(metric, BASE, 1), "org-a");
        batcher.enqueue(sample(metric, BASE, 1), "org-b");

        SentBatch first = sender.awaitNext();
        SentBatch second = sender.awaitNext();
        List<String> orgs = List.of(first.organizationId, second.organizationId);
        assertTrue(orgs.contains("org-a"));
        assertTrue(orgs.contains("org-b"));
    }

    // ------------------------------------------------------------------
    // Ordering
    // ------------------------------------------------------------------

    @Test
    public void keepsPerSeriesOrderAcrossBatches() throws Exception {
        // Block the first send so a second batch builds up behind it; the shard must not deliver
        // the second batch until the first has been acknowledged.
        sender.blockFirstSend();
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(50).build();

        Metric metric = gauge("ordered_series");
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 1), null);
        sender.awaitSendEntered();
        batcher.enqueue(sample(metric, BASE.plusSeconds(2), 2), null);
        // Nothing else may arrive while the first send is blocked.
        Thread.sleep(200);
        assertEquals(1, sender.sendsStarted());
        sender.releaseBlockedSend();

        SentBatch first = sender.awaitNext();
        SentBatch second = sender.awaitNext();
        assertEquals(BASE.plusSeconds(1).toEpochMilli(), first.request.getTimeseries(0).getSamples(0).getTimestamp());
        assertEquals(BASE.plusSeconds(2).toEpochMilli(), second.request.getTimeseries(0).getSamples(0).getTimestamp());
    }

    // ------------------------------------------------------------------
    // Failure semantics
    // ------------------------------------------------------------------

    @Test
    public void retriesTheSameBatchOnRetryableFailure() {
        sender.failNextSends(1, new RetryableWriteException("simulated 503"));
        batcher = builder().shardCount(1).maxBatchSamples(2).lingerMs(60_000)
                .maxRetries(3).retryBackoffMs(10).build();

        Metric metric = gauge("retried_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null);

        SentBatch sent = sender.awaitNext();
        assertEquals(2, sent.request.getTimeseries(0).getSamplesCount());
        assertEquals(2, sender.sendsStarted());
        assertEquals(1, registry.meter("batch.retries").getCount());
        assertEquals(2, registry.meter("samplesWritten").getCount());
        assertEquals(0, registry.meter("samplesLost").getCount());
    }

    @Test
    public void dropsTheBatchWhenRetriesAreExhaustedAndMovesOn() {
        sender.failNextSends(10, new RetryableWriteException("simulated persistent 503"));
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(60_000)
                .maxRetries(1).retryBackoffMs(10).build();

        Metric metric = gauge("doomed_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 1);

        // The shard survives and delivers the next batch once the backend recovers.
        sender.failNextSends(0, null);
        batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null);
        SentBatch sent = sender.awaitNext();
        assertEquals(BASE.plusSeconds(1).toEpochMilli(), sent.request.getTimeseries(0).getSamples(0).getTimestamp());
    }

    @Test
    public void dropsTheBatchImmediatelyOnFatalFailure() {
        sender.failNextSends(1, new StorageException("simulated 400"));
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(60_000)
                .maxRetries(5).retryBackoffMs(10_000).build();

        Metric metric = gauge("rejected_series");
        batcher.enqueue(sample(metric, BASE, 1), null);
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.meter("samplesLost").getCount() == 1);
        // A fatal failure must not be retried: one send, no retry marks.
        assertEquals(1, sender.sendsStarted());
        assertEquals(0, registry.meter("batch.retries").getCount());
    }

    @Test
    public void reportsEnqueueFailureWhenAShardStaysFull() {
        sender.blockFirstSend();
        batcher = builder().shardCount(1).maxBatchSamples(1).lingerMs(10)
                .shardCapacity(1).enqueueTimeoutMs(50).build();

        Metric metric = gauge("saturated_series");
        // First sample: flushed immediately and stuck in the blocked sender.
        assertTrue(batcher.enqueue(sample(metric, BASE, 1), null));
        sender.awaitSendEntered();
        // Second sample: sits in the shard queue (capacity 1).
        assertTrue(batcher.enqueue(sample(metric, BASE.plusSeconds(1), 2), null));
        // Third sample: no room, enqueue times out and the sample counts as lost.
        assertFalse(batcher.enqueue(sample(metric, BASE.plusSeconds(2), 3), null));
        assertEquals(1, registry.meter("samplesLost").getCount());

        sender.releaseBlockedSend();
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Test
    public void destroyDrainsBufferedSamples() {
        batcher = builder().shardCount(2).maxBatchSamples(1000).lingerMs(60_000).build();

        Metric metric = gauge("drained_series");
        for (int i = 0; i < 10; i++) {
            batcher.enqueue(sample(metric, BASE.plusSeconds(i), i), null);
        }
        batcher.destroy();

        int delivered = 0;
        for (SentBatch sent : sender.all()) {
            for (PrometheusTypes.TimeSeries ts : sent.request.getTimeseriesList()) {
                assertAscending(ts);
                delivered += ts.getSamplesCount();
            }
        }
        assertEquals(10, delivered);
        assertEquals(10, registry.meter("samplesWritten").getCount());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void assertAscending(final PrometheusTypes.TimeSeries ts) {
        for (int i = 1; i < ts.getSamplesCount(); i++) {
            assertTrue("samples must be in timestamp order per series",
                    ts.getSamples(i - 1).getTimestamp() <= ts.getSamples(i).getTimestamp());
        }
    }

    private static Metric gauge(final String name) {
        return ImmutableMetric.builder()
                .intrinsicTag("resourceId", "test/" + name)
                .intrinsicTag("name", name)
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();
    }

    private static Sample sample(final Metric metric, final Instant time, final double value) {
        return ImmutableSample.builder().metric(metric).time(time).value(value).build();
    }

    private static final class SentBatch {
        private final PrometheusRemote.WriteRequest request;
        private final String organizationId;

        SentBatch(final PrometheusRemote.WriteRequest request, final String organizationId) {
            this.request = request;
            this.organizationId = organizationId;
        }
    }

    /**
     * Records every completed send in arrival order. Can block the first send to expose the
     * single-flight invariant, and fail the next N sends with a given exception.
     */
    private static final class RecordingSender implements RemoteWriteSender {
        private final Queue<SentBatch> sent = new ConcurrentLinkedQueue<>();
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger failuresLeft = new AtomicInteger();
        private volatile StorageException failure;
        private volatile CountDownLatch blockEntered;
        private volatile CountDownLatch blockRelease;

        @Override
        public void send(final PrometheusRemote.WriteRequest writeRequest, final String organizationId)
                throws StorageException {
            started.incrementAndGet();
            final CountDownLatch entered = blockEntered;
            if (entered != null) {
                blockEntered = null;
                entered.countDown();
                try {
                    if (!blockRelease.await(10, TimeUnit.SECONDS)) {
                        throw new StorageException("test sender was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("interrupted in test sender");
                }
            }
            if (failuresLeft.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw failure;
            }
            sent.add(new SentBatch(writeRequest, organizationId));
        }

        void blockFirstSend() {
            blockRelease = new CountDownLatch(1);
            blockEntered = new CountDownLatch(1);
        }

        void awaitSendEntered() {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> started.get() > 0);
        }

        void releaseBlockedSend() {
            blockRelease.countDown();
        }

        void failNextSends(final int count, final StorageException e) {
            this.failure = e;
            this.failuresLeft.set(count);
        }

        int sendsStarted() {
            return started.get();
        }

        SentBatch awaitNext() {
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !sent.isEmpty());
            return sent.poll();
        }

        List<SentBatch> all() {
            return new ArrayList<>(sent);
        }
    }
}
