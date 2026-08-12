/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2026 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2026 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.timeseries.cortex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.xerial.snappy.Snappy;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

/**
 * Exercises {@link CortexTSS#store(java.util.List)} against a stub remote-write endpoint.
 *
 * Prior to this class, {@code store()} had no unit coverage at all: {@link CortexTSSTest} only
 * exercises static helpers, and every path through {@code store()} was reachable only from the
 * Docker-backed tests, which each perform a single write from a single thread.
 *
 * The tests are grouped by the behaviour they pin down: wire format, per-request timestamp sorting,
 * NaN filtering, the samplesWritten/samplesLost meters, failure propagation, timeout handling,
 * backpressure and the asyncWrites escape hatch.
 *
 * Two concurrency tests are {@link Ignore}d permanently; see the comment above them for why.
 */
public class CortexTSSStoreTest {

    private static final String WRITE_PATH = "/api/prom/push";
    private static final String READ_PATH = "/prometheus/api/v1";

    /** Long enough that the OkHttp read timeout never fires first in the timeout/interrupt tests. */
    private static final long GENEROUS_READ_TIMEOUT_MS = 30_000;

    private MockWebServer server;
    private CortexTSS tss;

    /**
     * snappy-java extracts and loads a native library on first use. Several tests below assert on
     * elapsed wall-clock time around a {@code store()} call, and JUnit does not run methods in
     * source order, so that one-off cost must not land inside a timed region.
     */
    @BeforeClass
    public static void warmUpSnappy() throws IOException {
        Snappy.compress(new byte[] {1, 2, 3});
    }

    @Before
    public void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        if (tss != null) {
            tss.destroy();
            tss = null;
        }
        server.shutdown();
    }

    // ------------------------------------------------------------------
    // Happy path -- passes on any implementation, guards the wire format
    // ------------------------------------------------------------------

    @Test
    public void writesASnappyCompressedProtobufToTheRemoteWriteEndpoint() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder());

        Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Metric metric = gauge("some_metric");
        tss.store(List.of(sample(metric, t, 1.0), sample(metric, t.plusSeconds(1), 2.0)));

        RecordedRequest request = takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals(WRITE_PATH, request.getPath());
        assertEquals("snappy", request.getHeader("Content-Encoding"));
        assertEquals("0.1.0", request.getHeader("X-Prometheus-Remote-Write-Version"));
        assertEquals(CortexTSS.class.getCanonicalName(), request.getHeader("User-Agent"));

        List<PrometheusTypes.TimeSeries> series = decode(request).getTimeseriesList();
        assertEquals(2, series.size());
        assertEquals(t.toEpochMilli(), series.get(0).getSamples(0).getTimestamp());
        assertEquals(t.plusSeconds(1).toEpochMilli(), series.get(1).getSamples(0).getTimestamp());
    }

    @Test
    public void sortsSamplesByTimestampWithinASingleWriteRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder());

        Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Metric metric = gauge("some_metric");
        tss.store(List.of(
                sample(metric, t.plusSeconds(3), 3.0),
                sample(metric, t.plusSeconds(1), 1.0),
                sample(metric, t.plusSeconds(2), 2.0)));

        List<Long> timestamps = new ArrayList<>();
        for (PrometheusTypes.TimeSeries s : decode(takeRequest()).getTimeseriesList()) {
            timestamps.add(s.getSamples(0).getTimestamp());
        }
        assertEquals(List.of(t.plusSeconds(1).toEpochMilli(),
                t.plusSeconds(2).toEpochMilli(),
                t.plusSeconds(3).toEpochMilli()), timestamps);
    }

    @Test
    public void setsTheOrgIdHeaderWhenAnOrganizationIsConfigured() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder().organizationId("tenant-a"));

        tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));

        assertEquals("tenant-a", takeRequest().getHeader("X-Scope-OrgID"));
    }

    /**
     * If nothing survives the NaN filter there is nothing to store. Posting an empty WriteRequest
     * would cost a round trip and, on the synchronous path, block a writer thread and potentially
     * throw for a batch that contained no storable data.
     */
    @Test
    public void doesNotWriteAtAllWhenEverySampleIsNaN() throws Exception {
        tss = storage(CortexTSSConfig.builder());

        Metric metric = gauge("some_metric");
        Instant t = Instant.now();
        tss.store(List.of(sample(metric, t, Double.NaN), sample(metric, t.plusSeconds(1), Double.NaN)));

        assertEquals("expected no request at all for an all-NaN batch", 0, server.getRequestCount());
        assertEquals(0, meter("samplesWritten"));
        assertEquals(0, meter("samplesLost"));
    }

    @Test
    public void omitsNaNSamplesFromTheWriteRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder());

        tss.store(samplesWithNaN(gauge("some_metric"), Instant.now().truncatedTo(ChronoUnit.SECONDS)));

        assertEquals(4, decode(takeRequest()).getTimeseriesCount());
    }

    // ------------------------------------------------------------------
    // Metrics -- the samplesWritten/samplesLost meters have never been asserted
    // ------------------------------------------------------------------

    @Test
    public void countsOnlyTheSamplesActuallySentAsWritten() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder());

        tss.store(samplesWithNaN(gauge("some_metric"), Instant.now().truncatedTo(ChronoUnit.SECONDS)));

        // 7 samples in, 3 of them NaN -- only the 4 real ones were sent.
        awaitMeter("samplesWritten", 4);
        assertEquals(0, meter("samplesLost"));
    }

    /**
     * Regression guard for the {@code samplesLost.mark(samples.size())} bug: the meter must count
     * the samples that were actually attempted, not the raw input including the NaNs that were
     * filtered out before the request was built.
     */
    @Test
    public void countsOnlyTheSamplesActuallySentAsLost() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("boom"));
        tss = storage(CortexTSSConfig.builder());

        List<Sample> samples = samplesWithNaN(gauge("some_metric"), Instant.now().truncatedTo(ChronoUnit.SECONDS));
        // the throw is this path's contract; see throwsWhenTheBackendRejectsTheWrite
        assertThrows(StorageException.class, () -> tss.store(samples));

        assertEquals(4, meter("samplesLost"));
        assertEquals(0, meter("samplesWritten"));
    }

    // ------------------------------------------------------------------
    // Failure propagation
    // ------------------------------------------------------------------

    /**
     * A rejected write must surface to the caller. While {@code store()} swallows the failure the
     * OpenNMS writer records the batch as persisted, so silent data loss is indistinguishable from
     * a healthy system -- and no integration test can see it either.
     */
    @Test
    public void throwsWhenTheBackendRejectsTheWrite() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("out of order sample"));
        tss = storage(CortexTSSConfig.builder());

        List<Sample> samples = List.of(sample(gauge("some_metric"), Instant.now(), 1.0));
        StorageException ex = assertThrows(StorageException.class, () -> tss.store(samples));
        assertTrue("expected the backend response to survive into the exception chain, got: " + ex,
                rootMessage(ex).contains("500") || rootMessage(ex).contains("out of order sample"));
    }

    @Test
    public void throwsWhenTheBackendIsUnreachable() throws Exception {
        server.shutdown(); // nothing is listening on the port any more
        tss = storage(CortexTSSConfig.builder());

        List<Sample> samples = List.of(sample(gauge("some_metric"), Instant.now(), 1.0));
        assertThrows(StorageException.class, () -> tss.store(samples));
    }

    // ------------------------------------------------------------------
    // Timeout handling
    // ------------------------------------------------------------------

    /**
     * When the write is abandoned on a timeout the underlying call must be cancelled. Otherwise the
     * request can still land at the backend after {@code store()} has reported the batch lost --
     * which both over-reports {@code samplesLost} and re-introduces the out-of-order write the
     * synchronous change is meant to prevent.
     */
    @Test
    public void cancelsTheInFlightCallWhenTheWriteTimesOut() {
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        tss = storage(CortexTSSConfig.builder()
                .callTimeoutInMs(500)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        List<Sample> samples = List.of(sample(gauge("some_metric"), Instant.now(), 1.0));
        assertThrows(StorageException.class, () -> tss.store(samples));

        // Without this the test would also pass if store() had failed before ever dispatching a
        // call -- runningCallsCount would be 0 because nothing ever ran.
        assertEquals("expected the request to have reached the server before the timeout fired",
                1, server.getRequestCount());

        Awaitility.await("in-flight call to be cancelled after the timeout")
                .atMost(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertEquals(Integer.valueOf(0), gaugeValue("runningCallsCount")));
    }

    /**
     * The timeout budget must cover the whole call -- connect, write, server processing and the
     * read of the ack -- not just the body write. Reusing {@code writeTimeoutInMs} (which is also
     * handed to {@code OkHttpClient.writeTimeout}) conflates the two.
     */
    @Test
    public void appliesACallTimeoutThatCoversServerProcessingTime() {
        // The body is written immediately; the server then sits on the response. OkHttp's
        // writeTimeout is satisfied, so only a genuine call-level timeout can bound this.
        server.enqueue(new MockResponse().setResponseCode(200)
                .setHeadersDelay(3, TimeUnit.SECONDS));
        tss = storage(CortexTSSConfig.builder()
                .callTimeoutInMs(500)
                .writeTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        List<Sample> samples = List.of(sample(gauge("some_metric"), Instant.now(), 1.0));
        long start = System.nanoTime();
        assertThrows(StorageException.class, () -> tss.store(samples));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue("expected store() to give up well before the server responded, took " + elapsedMs + "ms",
                elapsedMs < 2_000);
    }

    /**
     * {@code catch (Exception)} around a blocking {@code get()} swallows {@link InterruptedException}.
     * The interrupt flag must be restored, otherwise {@code RingBufferTimeseriesWriter.destroy()},
     * which relies on {@code ExecutorService.shutdownNow()}, cannot stop a parked writer thread.
     */
    @Test
    public void restoresTheInterruptFlagWhenTheStoringThreadIsInterrupted() throws Exception {
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        tss = storage(CortexTSSConfig.builder()
                .callTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .writeTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        CountDownLatch inStore = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicBoolean interruptFlagStillSet = new AtomicBoolean(false);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread writer = new Thread(() -> {
            inStore.countDown();
            try {
                tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                interruptFlagStillSet.set(Thread.currentThread().isInterrupted());
                done.countDown();
            }
        }, "store-under-test");
        writer.start();

        assertTrue(inStore.await(5, TimeUnit.SECONDS));
        Thread.sleep(500); // let the call get as far as blocking on the response
        writer.interrupt();

        assertTrue("store() did not return within 5s of being interrupted", done.await(5, TimeUnit.SECONDS));

        // Without this the test would pass if store() had simply returned normally, never entering
        // the InterruptedException branch it exists to pin.
        assertTrue("expected store() to fail with StorageException on interrupt, got: " + thrown.get(),
                thrown.get() instanceof StorageException);
        assertTrue("expected the interrupt flag to be restored, but it was cleared. thrown=" + thrown.get(),
                interruptFlagStillSet.get());
    }

    // ------------------------------------------------------------------
    // Concurrency -- NMS-19647
    //
    // Both tests below are @Ignored permanently. They describe a guarantee the plugin cannot make
    // on its own, and they are kept as an executable statement of that limitation rather than
    // deleted.
    //
    // Serialisation is not reordering. OpenNMS dispatches consecutive batches across
    // writer_threads (16 by default) Disruptor handlers, so batch N and batch N+1 race from the
    // moment they are handed out. Whichever thread reaches store() first writes first, whatever
    // store() does internally -- blocking, a global lock and sharding by series key all leave that
    // race untouched. Measured: 177/200 requests arrive out of order with fire-and-forget writes,
    // 90/200 with synchronous ones.
    //
    // The remedy is backend-side: configure an out-of-order time window (see the ordering section
    // window; see issue #132). These become live if the order-preserving write buffer discussed in #132 is
    // ever built.
    // ------------------------------------------------------------------

    /**
     * Per-series ordering across consecutive WriteRequests only holds if at most one request for a
     * given series is in flight at a time. This pins the invariant rather than the implementation --
     * it passes for any design that serialises a series.
     */
    @Test
    @Ignore("Requires ordering the plugin cannot guarantee - see the comment above and #132")
    public void neverHasTwoWritesForTheSameSeriesInFlightAtOnce() throws Exception {
        int threads = 8;
        int batchesPerThread = 10;

        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        server.setDispatcher(new okhttp3.mockwebserver.Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                int now = inFlight.incrementAndGet();
                maxInFlight.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(20); // hold the request open so overlap is observable
                    return new MockResponse().setResponseCode(200);
                } finally {
                    inFlight.decrementAndGet();
                }
            }
        });

        tss = storage(CortexTSSConfig.builder(), new ConcurrentKVStoreMock());
        runConcurrentStores(threads, batchesPerThread, gauge("shared_series"));

        assertEquals("expected writes for one series to be serialised, but up to "
                + maxInFlight.get() + " were in flight at once", 1, maxInFlight.get());
    }

    /**
     * The symptom the backend reports: a WriteRequest arriving with a timestamp older than one the
     * backend has already accepted for that series.
     */
    @Test
    @Ignore("Requires ordering the plugin cannot guarantee - see the comment above and #132")
    public void deliversWriteRequestsForASeriesInNonDecreasingTimestampOrder() throws Exception {
        int threads = 8;
        int batchesPerThread = 25;

        List<Long> arrivalOrder = Collections.synchronizedList(new ArrayList<>());
        server.setDispatcher(new okhttp3.mockwebserver.Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                try {
                    PrometheusRemote.WriteRequest wr = decode(request);
                    long maxTs = wr.getTimeseriesList().stream()
                            .mapToLong(ts -> ts.getSamples(0).getTimestamp())
                            .max().orElse(Long.MIN_VALUE);
                    arrivalOrder.add(maxTs);
                } catch (IOException e) {
                    return new MockResponse().setResponseCode(400);
                }
                Thread.sleep(5);
                return new MockResponse().setResponseCode(200);
            }
        });

        tss = storage(CortexTSSConfig.builder(), new ConcurrentKVStoreMock());
        runConcurrentStores(threads, batchesPerThread, gauge("shared_series"));

        List<Long> outOfOrder = new ArrayList<>();
        long highWaterMark = Long.MIN_VALUE;
        synchronized (arrivalOrder) {
            for (Long ts : arrivalOrder) {
                if (ts < highWaterMark) {
                    outOfOrder.add(ts);
                }
                highWaterMark = Math.max(highWaterMark, ts);
            }
        }
        assertEquals(outOfOrder.size() + " of " + arrivalOrder.size()
                + " write requests arrived out of timestamp order: " + outOfOrder, 0, outOfOrder.size());
    }

    /**
     * A call timeout OkHttp cannot use must not take the plugin down. Zero reads as "no timeout at
     * all" and anything above Integer.MAX_VALUE millis is rejected outright -- and the README shows
     * Long.MAX_VALUE for a neighbouring property, so both are a plausible config:edit away. In OSGi
     * a throw here would fail the blueprint bean and the TimeSeriesStorage service would never
     * register, taking all metric storage with it.
     */
    @Test
    public void clampsAnUnusableCallTimeoutInsteadOfFailingToStart() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder().callTimeoutInMs(Long.MAX_VALUE));
        tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));
        assertNotNull(takeRequest());

        tss.destroy();
        server.enqueue(new MockResponse().setResponseCode(200));
        tss = storage(CortexTSSConfig.builder().callTimeoutInMs(0));
        tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));
        assertNotNull(takeRequest());
    }

    /**
     * The interrupt path abandons the write, so it must also cancel it. Otherwise the request lands
     * after the batch has been reported lost -- over-counting samplesLost and producing exactly the
     * late, out-of-order write the synchronous path exists to avoid.
     */
    @Test
    public void cancelsTheInFlightCallWhenTheStoringThreadIsInterrupted() throws Exception {
        // NO_RESPONSE rather than a delayed response: the request is still recorded, but no server
        // thread parks on it, so tearDown can shut the server down promptly.
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        tss = storage(CortexTSSConfig.builder()
                .callTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .writeTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        CountDownLatch done = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            try {
                tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));
            } catch (Throwable ignored) {
                // asserted via the gauge below
            } finally {
                done.countDown();
            }
        }, "store-under-test");
        writer.start();

        Awaitility.await("the write to be in flight")
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> server.getRequestCount() == 1);
        writer.interrupt();
        assertTrue("store() did not return within 5s of being interrupted", done.await(5, TimeUnit.SECONDS));

        Awaitility.await("the abandoned call to be cancelled rather than left running")
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertEquals(Integer.valueOf(0), gaugeValue("runningCallsCount")));
    }

    // ------------------------------------------------------------------
    // Backpressure
    // ------------------------------------------------------------------

    /**
     * A synchronous {@code store()} caps in-flight writes at the number of OpenNMS writer threads
     * (16) rather than the bulkhead's limit, and parks each of them while the backend is unhealthy.
     * That is the accepted cost of reporting failures to the caller -- but it has to stay bounded,
     * because once all 16 writers are parked the 8192-entry ring buffer fills and samples are
     * dropped upstream of the plugin, where {@code samplesLost} cannot see them.
     *
     * The bound is the property worth guarding, not the absence of blocking. If this test needs
     * relaxing, the asyncWrites guidance in the README has to change with it.
     */
    @Test
    public void parksTheCallingThreadNoLongerThanTheCallTimeout() {
        long callTimeoutMs = 1_000;
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        tss = storage(CortexTSSConfig.builder()
                .callTimeoutInMs(callTimeoutMs)
                .writeTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        List<Sample> samples = List.of(sample(gauge("some_metric"), Instant.now(), 1.0));
        long start = System.nanoTime();
        assertThrows(StorageException.class, () -> tss.store(samples));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue("expected store() to wait for roughly the call timeout, but it returned after "
                + elapsedMs + "ms", elapsedMs >= callTimeoutMs / 2);
        assertTrue("expected store() to give up at the call timeout, but it blocked for "
                + elapsedMs + "ms", elapsedMs < callTimeoutMs * 3);
    }

    // ------------------------------------------------------------------
    // asyncWrites escape hatch -- the pre-2.2.0 behaviour, still supported
    // ------------------------------------------------------------------

    @Test
    public void asyncWritesReturnBeforeTheBackendResponds() throws Exception {
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        tss = storage(CortexTSSConfig.builder()
                .asyncWrites(true)
                .callTimeoutInMs(GENEROUS_READ_TIMEOUT_MS)
                .readTimeoutInMs(GENEROUS_READ_TIMEOUT_MS));

        long start = System.nanoTime();
        tss.store(List.of(sample(gauge("some_metric"), Instant.now(), 1.0)));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue("expected store() to return without waiting for the backend, took "
                + elapsedMs + "ms", elapsedMs < 1_000);
    }

    @Test
    public void asyncWritesDoNotThrowWhenTheBackendRejectsTheWrite() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("boom"));
        tss = storage(CortexTSSConfig.builder().asyncWrites(true));

        // no assertThrows: in this mode the failure is logged and counted, never reported
        tss.store(samplesWithNaN(gauge("some_metric"), Instant.now().truncatedTo(ChronoUnit.SECONDS)));

        awaitMeter("samplesLost", 4);
        assertEquals(0, meter("samplesWritten"));
    }

    // ------------------------------------------------------------------
    // Harness
    // ------------------------------------------------------------------

    private CortexTSS storage(CortexTSSConfig.Builder builder) {
        return storage(builder, new KVStoreMock());
    }

    private CortexTSS storage(CortexTSSConfig.Builder builder,
                              org.opennms.integration.api.v1.distributed.KeyValueStore kvStore) {
        return new CortexTSS(builder
                .writeUrl(server.url(WRITE_PATH).toString())
                .readUrl(server.url(READ_PATH).toString())
                .build(), kvStore);
    }

    /** Drives {@code threads} callers, each storing {@code batches} single-sample batches for {@code metric}. */
    private void runConcurrentStores(int threads, int batches, Metric metric) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger nextTimestampOffset = new AtomicInteger();
        Instant base = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    startGate.await();
                    for (int b = 0; b < batches; b++) {
                        Instant t = base.plusMillis(nextTimestampOffset.incrementAndGet() * 1000L);
                        tss.store(List.of(sample(metric, t, 1.0)));
                    }
                } catch (StorageException | InterruptedException e) {
                    // failures are reported by the assertions on what reached the server
                }
            }, pool));
        }

        startGate.countDown();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        pool.shutdownNow();

        // fire-and-forget writes may still be in flight once store() has returned
        int expected = threads * batches;
        Awaitility.await("all " + expected + " write requests to reach the server")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> server.getRequestCount() >= expected);
    }

    private RecordedRequest takeRequest() throws InterruptedException {
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        assertNotNull("no write request reached the server within 10s", request);
        return request;
    }

    private static PrometheusRemote.WriteRequest decode(RecordedRequest request) throws IOException {
        return PrometheusRemote.WriteRequest.parseFrom(Snappy.uncompress(request.getBody().readByteArray()));
    }

    private long meter(String name) {
        return tss.getMetrics().meter(name).getCount();
    }

    private void awaitMeter(String name, long expected) {
        Awaitility.await("meter " + name + " to reach " + expected)
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertEquals(expected, meter(name)));
    }

    private Object gaugeValue(String name) {
        return tss.getMetrics().getGauges().get(name).getValue();
    }

    private static Metric gauge(String name) {
        return ImmutableMetric.builder()
                .intrinsicTag("name", name)
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();
    }

    private static Sample sample(Metric metric, Instant time, double value) {
        return ImmutableSample.builder().metric(metric).time(time).value(value).build();
    }

    /** 7 samples, 3 of them NaN. */
    private static List<Sample> samplesWithNaN(Metric metric, Instant base) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            samples.add(sample(metric, base.plusSeconds(i), i % 2 == 1 ? Double.NaN : 42.3));
        }
        return samples;
    }

    private static String rootMessage(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append(' ');
        }
        return sb.toString();
    }

    /** {@link KVStoreMock} is backed by a plain HashMap and races under the concurrency tests. */
    private static final class ConcurrentKVStoreMock extends KVStoreMock {
        private final Map<String, Object> store = new ConcurrentHashMap<>();

        @Override
        public Optional get(String key, String context) {
            return Optional.ofNullable(store.get(key));
        }

        @Override
        public CompletableFuture<Long> putAsync(String key, Object value, String context) {
            store.put(key, value);
            return CompletableFuture.completedFuture(0L);
        }

        @Override
        public CompletableFuture<Map> enumerateContextAsync(String context) {
            return CompletableFuture.completedFuture(new ConcurrentHashMap<>(store));
        }
    }
}
