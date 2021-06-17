package org.opennms.timeseries.cortex;

import static org.opennms.timeseries.cortex.CortexTSS.INTRINSIC_TAG_NAMES;
import static org.opennms.timeseries.cortex.CortexTSS.METRIC_NAME_LABEL;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;

import prometheus.PrometheusTypes;

public class ResultMapper {

    private ResultMapper(){
    }

    public static List<Sample> fromRangeQueryResult(final String queryResult, final Metric metric) {
        return iteratorToFiniteStream(new JSONObject(queryResult)
                .getJSONObject("data")
                .getJSONArray("result")
                .iterator())
                .map(j -> toSamples(((JSONObject)j), metric))
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public static List<Sample> toSamples(final JSONObject result, final Metric metric) {
        // final org.opennms.integration.api.v1.timeseries.Metric metric = toMetricFromMap(result.getJSONObject("metric").toMap());
        return iteratorToFiniteStream(result.getJSONArray("values").iterator())
                .map(value -> toSample(metric, (JSONArray)value)).collect(Collectors.toList());
    }

    public static Sample toSample(final Metric metric, final JSONArray jarray) {
        return ImmutableSample.builder()
                .time(Instant.ofEpochSecond(jarray.getLong(0)))
                .value(jarray.getDouble(1))
                .metric(metric)
                .build();
    }

    public static List<Metric> fromSeriesQueryResult(final String queryResult) {
        return new JSONObject(queryResult)
                .getJSONArray("data")
                .toList()
                .stream().map(j -> toMetricFromMap(((Map<String, String>)j)))
                .collect(Collectors.toList());
    }

    public static <T> Metric toMetricFromMap(Map<String, T> tags) {
        ImmutableMetric.MetricBuilder metric = ImmutableMetric.builder();

        for (Map.Entry<String, T> entry : tags.entrySet()) {
            final String labelName = entry.getKey();
            final String labelValue = entry.getValue().toString();

            if (METRIC_NAME_LABEL.equals(labelName)) {
                metric.intrinsicTag(IntrinsicTagNames.name, labelValue);
            } else if (labelName.startsWith("_ext")) {
                metric.externalTag(externalLabelToTag(labelValue));
            } else if (INTRINSIC_TAG_NAMES.contains(labelName)) {
                metric.intrinsicTag(labelName, labelValue);
            } else  {
                metric.metaTag(labelName, labelValue);
            }
        }
        return metric.build();
    }

    static <T> Stream<T> iteratorToFiniteStream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    static PrometheusTypes.Label.Builder externalTagToLabel(final Tag tag) {
        // _ext_%hash% = %key%=%value%
        // %hash% is a hash of the value - the key contains special characters we want to preserve, so we place it in the value instead
        String name = "_ext_" + tag.getValue().hashCode();
        String value = tag.getKey() + "=" + tag.getValue();
        return PrometheusTypes.Label.newBuilder()
                .setName(name)
                .setValue(value);
    }

    static Tag externalLabelToTag(final String labelValue) {
        // _ext_%hash% = %key%=%value%
        // %hash% is a hash of the value - the key contains special characters we want to preserve, so we place it in the value instead
        String[] keyValue = labelValue.split("=");
        String key = keyValue[0];
        String value = keyValue[1];
        return new ImmutableTag(key, value);
    }
}
