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
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
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
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;

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
 *
 * @author jwhite
 */
public class CortexTSS implements TimeSeriesStorage {
    private static final Logger LOG = LoggerFactory.getLogger(CortexTSS.class);

    // Label name indicating the metric name of a timeseries.
    private static final String METRIC_NAME_LABEL = "__name__";

    // Metric names must match
    public static final Pattern METRIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z_:][a-zA-Z0-9_:]*$");
    // Label names must match
    public static final Pattern LABEL_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    private final static Set<String> INTRINSIC_TAG_NAMES = Sets.newHashSet(IntrinsicTagNames.name, IntrinsicTagNames.resourceId);

    private final OkHttpClient client;
    private final String writeUrl;

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");

    private final ManagedChannel channel;
    private final IngesterGrpc.IngesterBlockingStub blockingStub;

    public CortexTSS(String writeUrl, String ingressGrpcTarget) {
        this.writeUrl = Objects.requireNonNull(writeUrl);
        this.client = new OkHttpClient();

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(ingressGrpcTarget).usePlaintext();
        this.channel = channelBuilder.build();
        this.blockingStub = IngesterGrpc.newBlockingStub(channel);
    }

    @Override
    public void store(final List<Sample> samples) throws StorageException {
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

    /* We use gRPC for reading.
    public PrometheusRemote.ReadResponse read(PrometheusRemote.ReadRequest readRequest) throws IOException {
        byte[] compressed = Snappy.compress(readRequest.toByteArray());
        RequestBody body = RequestBody.create(PROTOBUF_MEDIA_TYPE, compressed);
        Request request = new Request.Builder()
                .url(readUrl)
                .addHeader("X-Prometheus-Remote-Read-Version", "0.1.0")
                .addHeader("Content-Encoding", "snappy")
                .addHeader("User-Agent", CortexTSS.class.getCanonicalName())
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
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
                throw new IOException("Oops: " + response.code() + " ah: " + response.message() + " oh: " + bodyMsg);
            }
            ResponseBody responseBody = response.body();
            if (responseBody != null) {
                try (InputStream is = responseBody.byteStream()) {
                    return PrometheusRemote.ReadResponse.parseFrom(is);
                }
            }
            return PrometheusRemote.ReadResponse.newBuilder().build();
        }
    }
    */

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

    public static String sanitizeMetricName(String metricName) {
        return metricName.replaceAll("[^a-zA-Z0-9_:]", "_");
    }

    public static String sanitizeLabelName(String labelName) {
        return labelName.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public List<Metric> getMetrics(Collection<Tag> tags) {
        LOG.info("Retrieving metrics for tags: {}", tags);

        Cortex.LabelMatchers.Builder matchersBuilder = Cortex.LabelMatchers.newBuilder();
        for (Tag tag : tags) {
            // Special handling for the metric name
            if (IntrinsicTagNames.name.equals(tag.getKey())) {
                matchersBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                        .setType(Cortex.MatchType.EQUAL)
                        .setName(METRIC_NAME_LABEL)
                        .setValue(sanitizeMetricName(tag.getValue())));
            } else {
                matchersBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                        .setType(Cortex.MatchType.EQUAL)
                        .setName(sanitizeLabelName(tag.getKey()))
                        .setValue(tag.getValue()));
            }
        }

        Cortex.MetricsForLabelMatchersRequest matchRequest = Cortex.MetricsForLabelMatchersRequest.newBuilder()
                .addMatchersSet(matchersBuilder)
                .build();
        Cortex.MetricsForLabelMatchersResponse response = blockingStub.metricsForLabelMatchers(matchRequest);
        return response.getMetricList().stream()
                .map(CortexTSS::toMetric)
                .collect(Collectors.toList());
    }

    protected static Metric toMetric(Cortex.Metric cortexMetric) {
        return toMetric(cortexMetric.getLabelsList());
    }

    private static Metric toMetric(List<Cortex.LabelPair> labelPairs) {
        final Set<Tag> intrinsicTags = new LinkedHashSet<>();
        final Set<Tag> metaTags = new LinkedHashSet<>();
        for (Cortex.LabelPair labelPair : labelPairs) {
            final String labelName = labelPair.getName().toStringUtf8();
            final String labelValue = labelPair.getValue().toStringUtf8();

            if (METRIC_NAME_LABEL.equals(labelName)) {
                intrinsicTags.add(new ImmutableTag(IntrinsicTagNames.name, labelValue));
                continue;
            }

            final Tag tag = new ImmutableTag(labelName, labelValue);
            if (INTRINSIC_TAG_NAMES.contains(labelName)) {
                intrinsicTags.add(tag);
            } else {
                metaTags.add(tag);
            }
        }
        return new ImmutableMetric(intrinsicTags, metaTags);
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) {
        LOG.info("Retrieving time series for metric: {}", request);

        Cortex.QueryRequest.Builder queryRequestBuilder = Cortex.QueryRequest.newBuilder()
                .setStartTimestampMs(request.getStart().toEpochMilli())
                .setEndTimestampMs(request.getEnd().toEpochMilli());

        // Convert all of the tags to labels
        request.getMetric().getIntrinsicTags().forEach(tag -> {
                    // Special handling for the metric name
                    if (IntrinsicTagNames.name.equals(tag.getKey())) {
                        queryRequestBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                                .setType(Cortex.MatchType.EQUAL)
                                .setName(METRIC_NAME_LABEL)
                                .setValue(sanitizeMetricName(tag.getValue())));
                    } else {
                        queryRequestBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                                .setType(Cortex.MatchType.EQUAL)
                                .setName(sanitizeLabelName(tag.getKey()))
                                .setValue(tag.getValue()));
                    }
                });

        Cortex.QueryRequest queryRequest = queryRequestBuilder.build();
        LOG.info("Retrieving time series for request: {} with query: {}", request, queryRequest);

        Cortex.QueryResponse queryResponse = blockingStub.query(queryRequest);
        if (queryResponse.getTimeseriesCount() < 1) {
            LOG.info("No matching series found.");
            return Collections.emptyList();
        }
        if (queryResponse.getTimeseriesCount() > 1) {
            LOG.info("Multiple series found, using the first.");
        }
        Cortex.TimeSeries timeSeries = queryResponse.getTimeseries(0);
        // Derive the metrics from the tags so that we include the type
        Metric metric = toMetric(timeSeries.getLabelsList());
        return timeSeries.getSamplesList().stream()
                .map(s -> ImmutableSample.builder()
                        .metric(metric)
                        .time(Instant.ofEpochMilli(s.getTimestampMs()))
                        .value(s.getValue())
                        .build())
                .collect(Collectors.toList());
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
