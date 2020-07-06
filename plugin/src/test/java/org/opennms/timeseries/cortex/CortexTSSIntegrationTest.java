package org.opennms.timeseries.cortex;

import java.io.File;
import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.timeseries.plugintest.AbstractStorageIntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class CortexTSSIntegrationTest extends AbstractStorageIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CortexTSSIntegrationTest.class);

    @ClassRule
    public static DockerComposeContainer environment = new DockerComposeContainer<>(new File("src/test/resources/org/opennms/timeseries/cortex/docker-compose.yaml"))
                .withExposedService("cortex", 9009, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
                .withExposedService("cortex", 9095, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)))
                .withExposedService("grafana", 3000, Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)));

    private CortexTSS cortexTss;

    @Before
    public void setUp() throws StorageException {
        cortexTss = new CortexTSS("http://localhost:9009/api/prom/push", "localhost:9095");
        super.setUp();
    }

    @Override
    protected TimeSeriesStorage createStorage() {
        return cortexTss;
    }

    @Override
    @Ignore // not yet implemented
    public void shouldDeleteMetrics() { }


    @After
    public void tearDown() {
        cortexTss.destroy();
    }
}
