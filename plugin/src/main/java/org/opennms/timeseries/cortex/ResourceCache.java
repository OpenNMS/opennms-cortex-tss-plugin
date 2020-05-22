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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.opennms.integration.api.v1.model.NodeCriteria;
import org.opennms.integration.api.v1.model.immutables.ImmutableNodeCriteria;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import cortex.Cortex;
import cortex.IngesterGrpc;

/**
 * This is used as a temporary measure to optimize the query performance. This should be handled
 * by the TSS API, so we don't need to have knowledge of this here.
 *
 * See https://issues.opennms.org/browse/NMS-12731 for details.
 *
 * Current impl:
 *   * Determine the node being queried - derive this from internal knowledge of the tags
 *   * Load all metrics for the node and cache these
 *   * Search the cache in memory
 */
public class ResourceCache {
    private final IngesterGrpc.IngesterBlockingStub blockingStub;

    private LoadingCache<NodeCriteria, NodeMetrics> metricsCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<NodeCriteria, NodeMetrics>() {
                        @Override
                        public NodeMetrics load(NodeCriteria nodeCriteria) {
                            return loadMetricsForNode(nodeCriteria);
                        }
                    });

    public ResourceCache(IngesterGrpc.IngesterBlockingStub blockingStub) {
        this.blockingStub = blockingStub;
    }

    private static class NodeMetrics {
        private List<Metric> metrics;

        public NodeMetrics(List<Metric> metrics) {
            this.metrics = metrics;
        }

        public List<Metric> match(Tag tag) {
            return metrics.stream()
                    .filter(m -> m.getMetaTags().contains(tag))
                    .collect(Collectors.toList());
        }
    }

    public List<Metric> getMetricsForNode(NodeCriteria nodeCriteria, Tag tag) {
        return metricsCache.getUnchecked(nodeCriteria).match(tag);
    }

    public NodeMetrics loadMetricsForNode(NodeCriteria nodeCriteria) {
        Cortex.LabelMatchers.Builder matchersBuilder = Cortex.LabelMatchers.newBuilder();

        if (nodeCriteria.getForeignId() != null) {
            matchersBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                    .setType(Cortex.MatchType.REGEX_MATCH)
                    .setName("_idx3")
                    .setValue(String.format("\\(snmp:fs:%s:%s,.*",
                            nodeCriteria.getForeignSource(), nodeCriteria.getForeignId())));
        } else {
            matchersBuilder.addMatchers(Cortex.LabelMatcher.newBuilder()
                    .setType(Cortex.MatchType.REGEX_MATCH)
                    .setName("_idx2")
                    .setValue(String.format("\\(snmp:%d,.*", nodeCriteria.getId())));
        }

        Cortex.MetricsForLabelMatchersRequest matchRequest = Cortex.MetricsForLabelMatchersRequest.newBuilder()
                .addMatchersSet(matchersBuilder)
                .build();
        Cortex.MetricsForLabelMatchersResponse response = blockingStub.metricsForLabelMatchers(matchRequest);
        List<Metric> metrics = response.getMetricList().stream()
                .map(CortexTSS::toMetric)
                .collect(Collectors.toList());
        return new NodeMetrics(metrics);
    }

    public static List<String> getResourceIdElementsFromTag(Tag tag) {
        if (!tag.getKey().startsWith("_idx") || !tag.getValue().startsWith("(")) {
            return Collections.emptyList();
        }
        int lastCommaIdx = tag.getValue().lastIndexOf(",");
        return ResourceIdSplitter.splitIdIntoElements(tag.getValue().substring(1,lastCommaIdx));
    }

    public static Optional<NodeCriteria> getNodeCriteriaFromTag(Tag tag) {
        final List<String> elements = ResourceCache.getResourceIdElementsFromTag(tag);
        if (elements.size() >= 2 && "snmp".equals(elements.get(0))) {
            if ("fs".equals(elements.get(1))) {
                // storeByFs=true
                String fs = elements.get(2);
                String fid = elements.get(3);
                return Optional.of(ImmutableNodeCriteria.newBuilder()
                        .setForeignSource(fs)
                        .setForeignId(fid)
                        .build());
            } else {
                // storeByFs=false
                int nodeId = Integer.parseInt(elements.get(2));
                return Optional.of(ImmutableNodeCriteria.newBuilder()
                        .setId(nodeId)
                        .build());
            }
        }
        return Optional.empty();
    }

    public static class ResourceIdSplitter {

        private static final Pattern MATCH_ESCAPED_COLON = Pattern.compile("\\\\:");
        private static final Pattern MATCH_ESCAPED_BACKSLASH = Pattern.compile("\\\\\\\\");

        public static List<String> splitIdIntoElements(String id) {
            Preconditions.checkNotNull(id, "id argument");
            List<String> elements = Lists.newArrayList();
            int startOfNextElement = 0;
            int numConsecutiveEscapeCharacters = 0;
            char[] idChars = id.toCharArray();

            for(int i = 0; i < idChars.length; ++i) {
                if (idChars[i] == ':' && numConsecutiveEscapeCharacters % 2 == 0) {
                    maybeAddSanitizedElement(new String(idChars, startOfNextElement, i - startOfNextElement), elements);
                    startOfNextElement = i + 1;
                }

                if (idChars[i] == '\\') {
                    ++numConsecutiveEscapeCharacters;
                } else {
                    numConsecutiveEscapeCharacters = 0;
                }
            }

            maybeAddSanitizedElement(new String(idChars, startOfNextElement, idChars.length - startOfNextElement), elements);
            return elements;
        }

        private static void maybeAddSanitizedElement(String element, List<String> elements) {
            String sanitizedElement = element.trim();
            if (sanitizedElement.length() != 0) {
                sanitizedElement = MATCH_ESCAPED_COLON.matcher(sanitizedElement).replaceAll(":");
                sanitizedElement = MATCH_ESCAPED_BACKSLASH.matcher(sanitizedElement).replaceAll("\\\\");
                elements.add(sanitizedElement);
            }
        }
    }

}
