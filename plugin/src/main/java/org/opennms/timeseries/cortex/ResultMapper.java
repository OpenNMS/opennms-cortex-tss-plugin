package org.opennms.timeseries.cortex;

import static org.opennms.timeseries.cortex.CortexTSS.INTRINSIC_TAG_NAMES;
import static org.opennms.timeseries.cortex.CortexTSS.METRIC_NAME_LABEL;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        final Set<Tag> intrinsicTags = new LinkedHashSet<>();
        final Set<Tag> metaTags = new LinkedHashSet<>();
        for (Map.Entry<String, T> entry : tags.entrySet()) {
            final String labelName = entry.getKey();
            final String labelValue = entry.getValue().toString();

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

    static <T> Stream<T> iteratorToFiniteStream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}
