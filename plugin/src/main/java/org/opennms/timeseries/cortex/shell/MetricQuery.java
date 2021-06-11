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

package org.opennms.timeseries.cortex.shell;


import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTagMatcher;
import org.opennms.timeseries.cortex.CortexTSS;

@Command(scope = "opennms-cortex", name = "query-metrics", description = "Find metrics.", detailedDescription= "pairs")
@Service
public class MetricQuery implements Action {

    @Reference
    private CortexTSS tss;

    @Argument(multiValued=true)
    private List<String> arguments = new LinkedList<>();

    @Override
    public Object execute() throws Exception {
        final List<TagMatcher> tags = toTags(arguments);
        System.out.println("Querying metrics for tags: " + tags);
        List<Metric> metrics = tss.findMetrics(tags);
        System.out.println("Metrics:");
        for (Metric metric : metrics) {
            System.out.println("\t" + metric);
        }
        if (metrics.isEmpty()) {
            System.out.println("(No results returned)");
        }
        return null;
    }

    public static List<TagMatcher> toTags(final Collection<String> s) {
        if (s.size() % 2 == 1) {
            throw new IllegalArgumentException("collection must have an even number of arguments");
        }
        final AtomicInteger counter = new AtomicInteger(0);
        return s.stream().collect(
                Collectors.groupingBy(el -> {
                    final int i = counter.getAndIncrement();
                    return (i % 2 == 0) ? i : i - 1;
                })).values().stream().map(a -> ImmutableTagMatcher.builder().key(a.get(0)).value((a.size() == 2 ? a.get(1) : null)).build())
                .collect(Collectors.toList());
    }
}
