package org.opennms.timeseries.cortex;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;

public class PushSample {
    public static void main(String[] args) throws StorageException {
        Random random = new Random(42);
        CortexTSS cortexTss = new CortexTSS("http://localhost:9009/api/prom/push", "http://localhost:9009/prometheus/api/v1");
        Metric metric = ImmutableMetric.builder()
                .intrinsicTag(IntrinsicTagNames.resourceId, "response:127.0.0.1:icmp")
                .intrinsicTag(IntrinsicTagNames.name, "icmp")
                .metaTag("_idx2", "(response:127.0.0.1:icmp,3)")
                .metaTag("mtype", "gauge")
                .metaTag("ICMP/127.0.0.1", "icmp")
                .metaTag("_idx0", "(response,3)")
                .metaTag("_idx1", "(response:127.0.0.1,3)")
                .metaTag("_idx2w", "(response:127.0.0.1,*)")
                .build();
        List<Sample> samples = new ArrayList<>();
        for(int i=10000; i>0; i--) {
            samples.add(ImmutableSample.builder()
                    .metric(metric)
                    .value((double)random.nextInt(8000 - 3000) + 3000)
                    .time(Instant.now().minus(i*10, ChronoUnit.MINUTES)).build());
        }
        cortexTss.store(samples);
    }
}
