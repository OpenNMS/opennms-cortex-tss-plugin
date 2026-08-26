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

/**
 * JSON shape of the plugin configuration, mirroring the OSGi config PID
 * {@code org.opennms.plugins.tss.prometheus}. Field names match the property
 * names used in the blueprint property-placeholder so the REST layer can map
 * straight to/from ConfigurationAdmin.
 */
public class PrometheusConfigDto {
    private String writeUrl;
    private String readUrl;
    private int maxConcurrentHttpConnections;
    private long writeTimeoutInMs;
    private long readTimeoutInMs;
    private long metricCacheSize;
    private long externalTagsCacheSize;
    private long bulkheadMaxWaitDuration;
    private boolean bulkheadUnlimited;
    private long maxSeriesLookback;
    private String organizationId;

    public PrometheusConfigDto() {}

    public String getWriteUrl() { return writeUrl; }
    public void setWriteUrl(String writeUrl) { this.writeUrl = writeUrl; }

    public String getReadUrl() { return readUrl; }
    public void setReadUrl(String readUrl) { this.readUrl = readUrl; }

    public int getMaxConcurrentHttpConnections() { return maxConcurrentHttpConnections; }
    public void setMaxConcurrentHttpConnections(int v) { this.maxConcurrentHttpConnections = v; }

    public long getWriteTimeoutInMs() { return writeTimeoutInMs; }
    public void setWriteTimeoutInMs(long v) { this.writeTimeoutInMs = v; }

    public long getReadTimeoutInMs() { return readTimeoutInMs; }
    public void setReadTimeoutInMs(long v) { this.readTimeoutInMs = v; }

    public long getMetricCacheSize() { return metricCacheSize; }
    public void setMetricCacheSize(long v) { this.metricCacheSize = v; }

    public long getExternalTagsCacheSize() { return externalTagsCacheSize; }
    public void setExternalTagsCacheSize(long v) { this.externalTagsCacheSize = v; }

    public long getBulkheadMaxWaitDuration() { return bulkheadMaxWaitDuration; }
    public void setBulkheadMaxWaitDuration(long v) { this.bulkheadMaxWaitDuration = v; }

    public boolean isBulkheadUnlimited() { return bulkheadUnlimited; }
    public void setBulkheadUnlimited(boolean v) { this.bulkheadUnlimited = v; }

    public long getMaxSeriesLookback() { return maxSeriesLookback; }
    public void setMaxSeriesLookback(long v) { this.maxSeriesLookback = v; }

    public String getOrganizationId() { return organizationId; }
    public void setOrganizationId(String organizationId) { this.organizationId = organizationId; }
}
