package org.opennms.timeseries.cortex;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class NMS16271_IT {
    @ClassRule
    public static DockerComposeContainer<?> environment = new DockerComposeContainer<>(new File("src/test/resources/org/opennms/timeseries/cortex/docker-compose.yaml"))
            .withExposedService("cortex", 9009, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
            .withExposedService("grafana", 3000, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)));

    @Test
    public void myTest() throws Exception {
        final TimeSeriesStorage storage = new CortexTSS(new CortexTSSConfig(), new KVStoreMock());
        final Instant referenceTime = Instant.now().with(ChronoField.MICRO_OF_SECOND, 0L);

        final Metric metric = ImmutableMetric.builder()
                .intrinsicTag("name", "name")
                .metaTag("mtype", Metric.Mtype.gauge.name())
                .build();

        final List<Sample> samplesIn = new ArrayList<>();

        // add samples including some NaN values
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(7, ChronoUnit.SECONDS)).value(42.3).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(6, ChronoUnit.SECONDS)).value(Double.NaN).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(5, ChronoUnit.SECONDS)).value(42.3).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(4, ChronoUnit.SECONDS)).value(Double.NaN).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(3, ChronoUnit.SECONDS)).value(42.3).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(2, ChronoUnit.SECONDS)).value(Double.NaN).metric(metric).build());
        samplesIn.add(ImmutableSample.builder().time(referenceTime.plus(1, ChronoUnit.SECONDS)).value(42.3).metric(metric).build());
        Assert.assertTrue(samplesIn.stream().anyMatch(s -> s.getValue().isNaN()));

        // store the samples
        storage.store(samplesIn);

        final TimeSeriesFetchRequest request = ImmutableTimeSeriesFetchRequest.builder().start(referenceTime).end(referenceTime.plusSeconds(10)).metric(metric).aggregation(Aggregation.NONE).step(Duration.of(1, ChronoUnit.SECONDS)).build();

        // wait till ten samples are available
        Awaitility.await()
                .atLeast(Duration.of(1, ChronoUnit.SECONDS))
                .atMost(Duration.of(10, ChronoUnit.SECONDS))
                .with()
                .pollInterval(Duration.of(1, ChronoUnit.SECONDS))
                .until(() -> storage.getTimeseries(request).size() == 10);

        // retrieve the samples
        final List<Sample> samplesOut = storage.getTimeseries(request);
        // ten samples available?
        Assert.assertTrue(samplesOut.size() == 10);
        // all values are 42.3, no NaN
        Assert.assertFalse(samplesOut.stream().anyMatch(s->!Double.valueOf(42.3).equals(s.getValue())));
    }
}
