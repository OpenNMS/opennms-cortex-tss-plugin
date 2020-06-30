package org.opennms.timeseries.cortex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.opennms.timeseries.plugintest.AbstractStorageIntegrationTest;

public class CortexTSSIntegrationTest extends AbstractStorageIntegrationTest {

    static Path tmpDir;
    protected static Process cortexDocker;
    protected static ExecutorService pool = Executors.newSingleThreadExecutor();
    private CortexTSS cortex;

    @BeforeClass
    public static void setUpOnce() throws IOException, InterruptedException {
        tmpDir = Files.createTempDirectory(CortexTSSIntegrationTest.class.getSimpleName());
        downloadFile(tmpDir, "https://raw.githubusercontent.com/opennms-forge/stack-play/master/standalone-cortex-minimal/docker-compose.yaml");
        downloadFile(tmpDir, "https://raw.githubusercontent.com/opennms-forge/stack-play/master/standalone-cortex-minimal/single-process-config.yaml");

        cortexDocker = new ProcessBuilder()
                .command("/usr/bin/bash", "-c", "docker-compose up")
                .directory(tmpDir.toFile())
                .redirectErrorStream(true)
                .inheritIO()
                .start();
        Thread.sleep(10000); // wait for docker to start
    }

    private static void downloadFile(final Path dir, final String url) throws IOException {
        ReadableByteChannel readableByteChannel = Channels.newChannel(new URL(url).openStream());
        String filename = url.substring(url.lastIndexOf('/'));
        FileOutputStream fileOutputStream = new FileOutputStream(new File(tmpDir.toFile(), filename));
        java.nio.channels.FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel().transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
    }

    @Before
    public void setUp() throws StorageException {
        cortex = new CortexTSS("http://localhost:9009/api/prom/push", "localhost:9095");
        super.setUp();
    }

    @Override
    protected TimeSeriesStorage createStorage() {
        return cortex;
    }

    @Override
    @Ignore // not yet implemented
    public void shouldDeleteMetrics() { }


    @After
    public void tearDown() {
        cortex.destroy();
    }

    @AfterClass
    public static void tearDownOnce() throws IOException, InterruptedException {
        cortexDocker.destroy();
        execute("docker stop grafana").waitFor();
        execute("docker stop cortex").waitFor();
        execute("docker rm grafana").waitFor();
        execute("docker rm cortex").waitFor();
    }

    private static Process execute(String cmd) throws IOException {
        return new ProcessBuilder()
                .command("/usr/bin/bash", "-c", cmd)
                .directory(tmpDir.toFile())
                .redirectErrorStream(true)
                .inheritIO()
                .start();
    }
}
