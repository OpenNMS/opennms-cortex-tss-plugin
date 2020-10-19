/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
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

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import cortex.Cortex;
import cortex.IngesterGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

/**
 * Time Series Storage integration for Cortex.
 * We use the cortex api to write data (writes to the ingester) and the prometheus api to read data (reads from the Querier).
 * Even though it's possible to read from the ingester it does only give us the most resent data (still held in memory) therefore
 * we use the querier.
 * Docs:
 * - https://cortexmetrics.io/docs/api/
 * - https://prometheus.io/docs/prometheus/latest/querying/api/
 *
 * @author jwhite
 */
public class CortexTSS implements TimeSeriesStorage {
    private static final Logger LOG = LoggerFactory.getLogger(CortexTSS.class);

    // Label name indicating the metric name of a timeseries.
    public static final String METRIC_NAME_LABEL = "__name__";
    private static final ByteString METRIC_NAME_LABEL_BYTE_STRING = ByteString.copyFromUtf8(METRIC_NAME_LABEL);

    // Metric names must match
    public static final Pattern METRIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z_:][a-zA-Z0-9_:]*$");
    // Label names must match
    public static final Pattern LABEL_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
    // Used to sanitize the metric name
    private static final Pattern SANITIZE_METRIC_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9_:]");
    // Used to sanitize the label name
    private static final Pattern SANITIZE_LABEL_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9_]");

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    public final static Set<String> INTRINSIC_TAG_NAMES = Sets.newHashSet(IntrinsicTagNames.name, IntrinsicTagNames.resourceId);

    private final OkHttpClient client;
    private final String writeUrl;
    private final String readUrl;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");

    private final ManagedChannel channel;
    private final IngesterGrpc.IngesterBlockingStub blockingStub;

    public CortexTSS(String writeUrl, String ingressGrpcTarget, final String readUrl) {
        this.writeUrl = Objects.requireNonNull(writeUrl);
        this.readUrl = Objects.requireNonNull(readUrl);
        this.client = new OkHttpClient();

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(ingressGrpcTarget)
                // TODO: Make this configurable?
                .maxInboundMessageSize(10 * 1024 * 1024)
                .usePlaintext();
        this.channel = channelBuilder.build();
        this.blockingStub = IngesterGrpc.newBlockingStub(channel);
    }

    @Override
    public void store(final List<Sample> samples) throws StorageException {
        storeSamplesViagRPC(samples);
    }

    private void storeSamplesViagRPC(final List<Sample> samples) throws StorageException {
        final List<Sample> samplesSorted = samples.stream() // Cortex doesn't like the Samples to be out of time order
                .sorted(Comparator.comparing(Sample::getTime))
                .collect(Collectors.toList());
        Cortex.WriteRequest.Builder writeBuilder = Cortex.WriteRequest.newBuilder();
        samplesSorted.forEach(s -> writeBuilder.addTimeseries(toCortexTimeSeries(s)));
        Cortex.WriteRequest writeRequest = writeBuilder.build();
        LOG.trace("Writing: {}", writeRequest);
        try {
            blockingStub.push(writeRequest);
            samplesWritten.mark(samples.size());
        } catch (Exception e) {
            throw new StorageException(String.format("Failed to write %d samples.", samples.size()), e);
        }
    }

    private void storeSamplesViaHTTP(final List<Sample> samples) throws StorageException {
        final List<Sample> samplesSorted = samples.stream() // Cortex doesn't like the Samples to be out of time order
                .sorted(Comparator.comparing(Sample::getTime))
                .collect(Collectors.toList());
        PrometheusRemote.WriteRequest.Builder writeBuilder = PrometheusRemote.WriteRequest.newBuilder();
        samplesSorted.forEach(s -> writeBuilder.addTimeseries(toTimeSeries(s)));
        PrometheusRemote.WriteRequest writeRequest = writeBuilder.build();
        LOG.trace("Writing: {}", writeRequest);
        try {
            write(writeRequest);
            samplesWritten.mark(samples.size());
        } catch (IOException e) {
            throw new StorageException(String.format("Failed to write %d samples.", samples.size()), e);
        }
    }

    public void write(PrometheusRemote.WriteRequest writeRequest) throws StorageException, IOException {
        byte[] compressed = Snappy.compress(writeRequest.toByteArray());
        RequestBody body = RequestBody.create(PROTOBUF_MEDIA_TYPE, compressed);
        Request request = new Request.Builder()
                .url(writeUrl)
                .addHeader("X-Prometheus-Remote-Write-Version", "0.1.0")
                .addHeader("Content-Encoding", "snappy")
                .addHeader("User-Agent", CortexTSS.class.getCanonicalName())
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new StorageException(String.format("Writing to Prometheus failed: %s - %s: %s",
                        response.code(),
                        response.message(), Optional
                                .ofNullable(response.body())
                                .map(this::readSilently)
                                .orElse("null")));
            }
        }
    }

    private String readSilently(final ResponseBody responseBody) {
        try {
            return responseBody.string();
        } catch (IOException ex) {
            return "null";
        }
    }

    private static PrometheusTypes.TimeSeries.Builder toTimeSeries(Sample sample) {
        PrometheusTypes.TimeSeries.Builder builder = PrometheusTypes.TimeSeries.newBuilder();
        // Convert all of the tags to labels
        Stream.concat(sample.getMetric().getIntrinsicTags().stream(), sample.getMetric().getMetaTags().stream())
                .forEach(tag -> {
                    // Special handling for the metric name
                    if (IntrinsicTagNames.name.equals(tag.getKey())) {
                        builder.addLabels(PrometheusTypes.Label.newBuilder()
                                .setName(METRIC_NAME_LABEL)
                                .setValue(sanitizeMetricName(tag.getValue())));
                    } else {
                        builder.addLabels(PrometheusTypes.Label.newBuilder()
                                .setName(sanitizeLabelName(tag.getKey()))
                                .setValue(tag.getValue()));
                    }
                });
        // Add the sample timestamp & value
        builder.addSamples(PrometheusTypes.Sample.newBuilder()
                .setTimestamp(sample.getTime().toEpochMilli())
                .setValue(sample.getValue()));
        return builder;
    }

    private static Cortex.TimeSeries.Builder toCortexTimeSeries(Sample sample) {
        Cortex.TimeSeries.Builder builder = Cortex.TimeSeries.newBuilder();
        // Convert all of the tags to labels
        Stream.concat(sample.getMetric().getIntrinsicTags().stream(), sample.getMetric().getMetaTags().stream())
                .forEach(tag -> {
                    // Special handling for the metric name
                    if (IntrinsicTagNames.name.equals(tag.getKey())) {
                        builder.addLabels(Cortex.LabelPair.newBuilder()
                                .setName(ByteString.copyFromUtf8(METRIC_NAME_LABEL))
                                .setValue(ByteString.copyFromUtf8(sanitizeMetricName(tag.getValue()))));
                    } else {
                        builder.addLabels(Cortex.LabelPair.newBuilder()
                                .setName(ByteString.copyFromUtf8(sanitizeLabelName(tag.getKey())))
                                .setValue(ByteString.copyFromUtf8(tag.getValue())));
                    }
                });
        // Add the sample timestamp & value
        builder.addSamples(Cortex.Sample.newBuilder()
                .setTimestampMs(sample.getTime().toEpochMilli())
                .setValue(sample.getValue()));
        return builder;
    }

    public static String sanitizeMetricName(String metricName) {
        return SANITIZE_METRIC_NAME_PATTERN.matcher(metricName).replaceAll("_");
    }

    public static String sanitizeLabelName(String labelName) {
        return SANITIZE_LABEL_NAME_PATTERN.matcher(labelName).replaceAll("_");
    }

    @Override
    public List<Metric> getMetrics(Collection<Tag> tags) throws StorageException {
        LOG.info("Retrieving metrics for tags: {}", tags);
        String url = String.format("%s/series?match[]={%s}",
                readUrl,
                tagsToQuery(tags));
        String json = makeCallToQueryApi(url);
        return ResultMapper.fromSeriesQueryResult(json);
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) throws StorageException {
        LOG.info("Retrieving time series for metric: {}", request);

        String url = String.format("%s/query_range?query={%s}&start=%s&end=%s&step=%ss",
                readUrl,
                tagsToQuery(request.getMetric().getIntrinsicTags()),
                request.getStart().getEpochSecond(),
                request.getEnd().getEpochSecond(),
                determineStepInSeconds(request)
        );
        String json = makeCallToQueryApi(url);
        return ResultMapper.fromRangeQueryResult(json);

    }

    private String makeCallToQueryApi(final String url) throws StorageException {

        Request httpRequest = new Request.Builder()
                .url(url)
                .addHeader("User-Agent", CortexTSS.class.getCanonicalName())
                .get()
                .build();
        try (Response response = client.newCall(httpRequest).execute()) {
            if (!response.isSuccessful()) {
                String bodyMsg = "";
                ResponseBody responseBody = response.body();
                if (responseBody != null) {
                    try {
                        bodyMsg = responseBody.string();
                    } finally {
                        responseBody.close();
                    }
                }
                throw new StorageException(String.format("Call to %s failed: response code:%s, response message:%s, bodyMessage:%s", url, response.code(), response.message(), bodyMsg));
            }
            ResponseBody responseBody = response.body();
            if (responseBody != null) {
                return responseBody.string();
            } else {
                throw new StorageException(String.format("Call to %s delivered no body.", url));
            }
        } catch (IOException | StorageException e) {
            throw new StorageException(String.format("Call to %s failed.", url), e);
        }
    }

    static long determineStepInSeconds(TimeSeriesFetchRequest request) {
        // step cannot be 0, Prometheus always aggregates in a range query.
        // so we try to calculate a small step but not too small for the range since we don't want to have too many results
        // the maximum seems to be 11,000
        if (request.getStep().getSeconds() > 0) {
            return request.getStep().getSeconds();
        }
        long durationInSeconds = request.getEnd().getEpochSecond() - request.getStart().getEpochSecond();
        double step = Math.ceil(durationInSeconds / 11000.0);
        return Math.max(1L, (long) step);
    }

    private String tagsToQuery(final Collection<Tag> tags) {
        StringBuilder b = new StringBuilder();
        for (Tag tag : tags) {
            String key;
            String value;
            if (IntrinsicTagNames.name.equals(tag.getKey())) {
                key = METRIC_NAME_LABEL;
                value = sanitizeMetricName(tag.getValue());
            } else {
                key = sanitizeLabelName(tag.getKey());
                value = tag.getValue();
            }
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append(key).append("=\"").append(value).append("\"");
        }
        return b.toString();
    }

    @Override
    public void delete(Metric metric) {
        LOG.warn("Deletes are not currently supported. Ignoring delete for: {}", metric);
        // delete can only be done when enabled:
        // --web.enable-admin-api flag to Prometheus through start-up script or docker-compose file, depending on installation method.
        // curl -X POST -g 'http://localhost:9090/api/v1/admin/tsdb/delete_series?match[]={foo="bar"}'

    }

    public void destroy() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.info("Interrupted while awaiting for channel to shutdown.");
        }
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }
}
