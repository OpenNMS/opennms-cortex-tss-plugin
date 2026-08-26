/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2026 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2026 The OpenNMS Group, Inc.
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
 *******************************************************************************/

package org.opennms.timeseries.cortex.rest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;

/**
 * Flattened, JSON-friendly view of a {@link Metric} returned by the explorer query.
 */
public class MetricDto {
    private String key;
    private String name;
    private Map<String, String> intrinsicTags = new LinkedHashMap<>();
    private Map<String, String> metaTags = new LinkedHashMap<>();
    private Map<String, String> externalTags = new LinkedHashMap<>();

    public MetricDto() {}

    public static MetricDto from(Metric metric) {
        MetricDto dto = new MetricDto();
        dto.key = metric.getKey();
        copyTags(metric.getIntrinsicTags(), dto.intrinsicTags);
        copyTags(metric.getMetaTags(), dto.metaTags);
        copyTags(metric.getExternalTags(), dto.externalTags);
        Tag nameTag = metric.getFirstTagByKey(org.opennms.integration.api.v1.timeseries.IntrinsicTagNames.name);
        dto.name = nameTag != null ? nameTag.getValue() : metric.getKey();
        return dto;
    }

    private static void copyTags(Set<Tag> tags, Map<String, String> target) {
        if (tags == null) {
            return;
        }
        for (Tag t : tags) {
            target.put(t.getKey(), t.getValue());
        }
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Map<String, String> getIntrinsicTags() { return intrinsicTags; }
    public void setIntrinsicTags(Map<String, String> v) { this.intrinsicTags = v; }

    public Map<String, String> getMetaTags() { return metaTags; }
    public void setMetaTags(Map<String, String> v) { this.metaTags = v; }

    public Map<String, String> getExternalTags() { return externalTags; }
    public void setExternalTags(Map<String, String> v) { this.externalTags = v; }
}
