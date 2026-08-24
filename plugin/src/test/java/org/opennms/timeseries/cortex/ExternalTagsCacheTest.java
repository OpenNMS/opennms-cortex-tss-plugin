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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

/**
 * Pins down the external tags cache population on the write path.
 *
 * {@code persistExternalTags()} runs for every stored sample. It used to populate the cache only
 * when the database record needed an update, so a metric whose record was already complete (the
 * steady state) was never cached, and every one of its samples paid a synchronous kvStore.get(),
 * a database round trip, forever. Observed in a production deployment as extTagsCacheMissed
 * tracking samplesWritten one to one at 19.5 million.
 */
public class ExternalTagsCacheTest {

    private MockWebServer server;
    private CountingKVStore kvStore;
    private CortexTSS tss;

    @Before
    public void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        kvStore = new CountingKVStore();
    }

    @After
    public void tearDown() throws Exception {
        if (tss != null) {
            tss.destroy();
            tss = null;
        }
        server.shutdown();
    }

    @Test
    public void cachesTheRecordAfterOneDatabaseReadWhenNoUpsertIsNeeded() throws Exception {
        Metric metric = metric("ifHCInOctets", "uplink");
        // The record already exists in the database and is complete: the steady state.
        kvStore.seed(metric.getKey(), "{\"ifAlias\":\"uplink\"}");
        tss = storage();

        Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        for (int i = 0; i < 3; i++) {
            server.enqueue(new MockResponse().setResponseCode(200));
            tss.store(List.of(sample(metric, t.plusSeconds(i), i)));
        }

        assertEquals("only the first sample may read the database", 1, kvStore.gets.get());
        assertEquals("a complete record must never be re-written", 0, kvStore.puts.get());
        assertEquals(1, meter("extTagsCacheMissed"));
        assertEquals(2, meter("extTagsCacheUsed"));
        assertEquals(0, meter("extTagsModified"));
    }

    @Test
    public void refreshesTheCacheWhenACacheHitNeedsAnUpsert() throws Exception {
        tss = storage();
        Instant t = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        // New metric: miss, absent in the database, created and cached.
        server.enqueue(new MockResponse().setResponseCode(200));
        tss.store(List.of(sample(metric("ifHCInOctets", "uplink"), t, 1)));
        assertEquals(1, kvStore.gets.get());
        assertEquals(1, kvStore.puts.get());

        // Same series gains a tag: cache hit, upsert, and the cached entry must be refreshed.
        Metric withExtra = ImmutableMetric.builder()
                .intrinsicTag("resourceId", "snmp/1/if1")
                .intrinsicTag("name", "ifHCInOctets")
                .metaTag("mtype", Metric.Mtype.counter.name())
                .externalTag("ifAlias", "uplink")
                .externalTag("ifDescr", "Ethernet1")
                .build();
        server.enqueue(new MockResponse().setResponseCode(200));
        tss.store(List.of(sample(withExtra, t.plusSeconds(1), 2)));
        assertEquals(2, kvStore.puts.get());
        assertEquals(1, meter("extTagsModified"));

        // Unchanged tags again: without the cache refresh above, this would upsert a third time.
        server.enqueue(new MockResponse().setResponseCode(200));
        tss.store(List.of(sample(withExtra, t.plusSeconds(2), 3)));
        assertEquals("an up-to-date cached record must not be re-written", 2, kvStore.puts.get());
        assertEquals("the cache must serve every lookup after the first", 1, kvStore.gets.get());
        assertEquals(1, meter("extTagsModified"));
    }

    // ------------------------------------------------------------------
    // Harness
    // ------------------------------------------------------------------

    private CortexTSS storage() {
        return new CortexTSS(CortexTSSConfig.builder()
                .writeUrl(server.url("/api/prom/push").toString())
                .readUrl(server.url("/prometheus/api/v1").toString())
                .build(), kvStore);
    }

    private long meter(String name) {
        return tss.getMetrics().meter(name).getCount();
    }

    private static Metric metric(String name, String ifAlias) {
        return ImmutableMetric.builder()
                .intrinsicTag("resourceId", "snmp/1/if1")
                .intrinsicTag("name", name)
                .metaTag("mtype", Metric.Mtype.counter.name())
                .externalTag("ifAlias", ifAlias)
                .build();
    }

    private static Sample sample(Metric metric, Instant time, double value) {
        return ImmutableSample.builder().metric(metric).time(time).value(value).build();
    }

    /**
     * Counts database reads and writes, and returns an empty map from the enumerate preload so the
     * constructor cannot mask the cache-miss path under test.
     */
    private static final class CountingKVStore extends KVStoreMock {
        private final AtomicInteger gets = new AtomicInteger();
        private final AtomicInteger puts = new AtomicInteger();

        void seed(String key, String json) {
            super.put(key, json, CortexTSS.CORTEX_TSS);
        }

        @Override
        public Optional get(String key, String context) {
            gets.incrementAndGet();
            return super.get(key, context);
        }

        @Override
        public CompletableFuture<Long> putAsync(String key, Object value, String context) {
            puts.incrementAndGet();
            return super.putAsync(key, value, context);
        }

        @Override
        public CompletableFuture<Map> enumerateContextAsync(String context) {
            return CompletableFuture.completedFuture(new HashMap<>());
        }
    }
}
