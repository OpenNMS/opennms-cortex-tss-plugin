package org.opennms.timeseries.cortex;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.time.Duration;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.opennms.integration.api.v1.timeseries.AbstractStorageIntegrationTest;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class CortexTSSIntegrationTest extends AbstractStorageIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CortexTSSIntegrationTest.class);

    @ClassRule
    public static DockerComposeContainer<?> environment = new DockerComposeContainer<>(new File("src/test/resources/org/opennms/timeseries/cortex/docker-compose.yaml"))
                .withExposedService("cortex", 9009, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
                .withExposedService("cortex", 9095, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
                .withExposedService("grafana", 3000, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)));

    private CortexTSS cortexTss;

    @Before
    public void setUp() throws StorageException {
        cortexTss = new CortexTSS("http://localhost:9009/api/prom/push", "localhost:9095", "http://localhost:9009/prometheus/api/v1");
        super.setUp();
    }

    @Override
    protected TimeSeriesStorage createStorage() {
        return cortexTss;
    }

    @Override
    public void shouldGetSamplesForMetric() throws StorageException {
        // The cortex query api doesn't allow to query for raw data for a range.
        // Therefore we need to adopt the test method.
        ImmutableMetric.MetricBuilder builder = ImmutableMetric.builder();
        ((Metric)this.metrics.get(0)).getIntrinsicTags().forEach(builder::intrinsicTag);
        Metric metric = builder.build();
        TimeSeriesFetchRequest request = ImmutableTimeSeriesFetchRequest.builder()
                .start(this.referenceTime.minusSeconds(300))
                .end(this.referenceTime)
                .metric(metric)
                .aggregation(Aggregation.NONE)
                .step(Duration.ZERO)
                .build();
        List<Sample> samples = storage.getTimeseries(request);
        // we expect 1 sample per second. The time series starts with the first recorded sample (60s ago) => 60 samples
        assertEquals(60, samples.size());
        Metric originalMetric = this.samplesOfFirstMetric.get(0).getMetric();

        for (Sample sample : samples) {
            assertEquals(originalMetric, sample.getMetric());
            // Metric.equals() doesn't include meta tags, therefore we need to test for it separately:
            assertEquals(originalMetric.getMetaTags(), sample.getMetric().getMetaTags());
            assertEquals(this.samplesOfFirstMetric.get(0).getValue(), sample.getValue());
        }
    }

    @Override
    @Ignore // not yet implemented
    public void shouldDeleteMetrics() { }


    @After
    public void tearDown() {
        cortexTss.destroy();
    }
}
