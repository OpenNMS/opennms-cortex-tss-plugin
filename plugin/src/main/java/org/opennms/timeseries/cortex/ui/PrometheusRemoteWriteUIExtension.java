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

package org.opennms.timeseries.cortex.ui;

import org.opennms.integration.api.v1.ui.UIExtension;

/**
 * Registers the plugin's Vue UI under Plugins &gt; "Prometheus RemoteWrite" in the
 * OpenNMS web shell. OpenNMS serves the built JS module from this bundle's
 * classpath at {@code getResourceRootPath()/getModuleFileName()}.
 */
public class PrometheusRemoteWriteUIExtension implements UIExtension {

    @Override
    public String getExtensionId() {
        return "prometheusremotewrite";
    }

    @Override
    public String getMenuEntry() {
        return "Prometheus RemoteWrite";
    }

    @Override
    public String getResourceRootPath() {
        return "prometheusremotewrite";
    }

    @Override
    public String getModuleFileName() {
        return "prometheusremotewrite.es.js";
    }

    @Override
    public Class<? extends UIExtension> getExtensionClass() {
        return PrometheusRemoteWriteUIExtension.class;
    }
}
