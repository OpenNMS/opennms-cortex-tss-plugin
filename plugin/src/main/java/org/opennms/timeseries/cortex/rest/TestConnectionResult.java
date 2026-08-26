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
 * Outcome of probing one endpoint (write or read) for reachability.
 * {@code reachable} means we received any HTTP response — even a 4xx/405 means
 * the host is up and routable, which is what we want to confirm.
 */
public class TestConnectionResult {
    private String endpoint;     // "write" | "read"
    private String url;
    private boolean reachable;
    private int statusCode;
    private long durationMs;
    private String detail;

    public TestConnectionResult() {}

    public TestConnectionResult(String endpoint, String url, boolean reachable, int statusCode, long durationMs, String detail) {
        this.endpoint = endpoint;
        this.url = url;
        this.reachable = reachable;
        this.statusCode = statusCode;
        this.durationMs = durationMs;
        this.detail = detail;
    }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public boolean isReachable() { return reachable; }
    public void setReachable(boolean reachable) { this.reachable = reachable; }

    public int getStatusCode() { return statusCode; }
    public void setStatusCode(int statusCode) { this.statusCode = statusCode; }

    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
}
