/*
 * Licensed to The OpenNMS Group, Inc (TOG) under one or more
 * contributor license agreements.  See the LICENSE.md file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * TOG licenses this file to You under the GNU Affero General
 * Public License Version 3 (the "License") or (at your option)
 * any later version.  You may not use this file except in
 * compliance with the License.  You may obtain a copy of the
 * License at:
 *
 *      https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package org.opennms.timeseries.cortex;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

/**
 * Publishes a {@link MetricRegistry} as MBeans on the platform MBean server using only the JDK's
 * {@code javax.management} API.
 *
 * <p>Deliberately not Dropwizard's JmxReporter: that lives in the separate metrics-jmx artifact,
 * and whether its classes are actually wireable inside the host's OSGi runtime depends on what
 * that container ships and exports. Observability that can silently fail to load is worse than
 * none - it reads as "zero problems" when it is actually "zero visibility". This class has no
 * dependency that can fail to resolve: {@code javax.management} is the JDK, and the metrics-core
 * types are ones the plugin already cannot run without.
 *
 * <p>Object names follow the exact convention Dropwizard's reporter uses -
 * {@code <domain>:name=<metric>,type=<meters|gauges>}, meters carrying a {@code Count} attribute
 * and gauges a {@code Value} - so collection configs and queries written against either are
 * interchangeable.
 *
 * <p>The metric set is snapshotted at {@link #start()}: every metric this plugin owns is
 * registered before the publisher starts, so there is nothing to track dynamically.
 */
final class RegistryJmxPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryJmxPublisher.class);

    private final MetricRegistry registry;
    private final String domain;
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private final List<ObjectName> registered = new ArrayList<>();

    RegistryJmxPublisher(final MetricRegistry registry, final String domain) {
        this.registry = Objects.requireNonNull(registry);
        this.domain = Objects.requireNonNull(domain);
    }

    /**
     * Registers one MBean per metric. A metric that cannot be registered is logged and skipped;
     * one bad name must not cost the visibility of the rest.
     */
    void start() {
        for (Map.Entry<String, Metric> entry : registry.getMetrics().entrySet()) {
            final DynamicMBean bean = mbeanFor(entry.getValue());
            if (bean == null) {
                LOG.debug("Metric {} has a type this publisher does not map; skipping it.", entry.getKey());
                continue;
            }
            try {
                final Hashtable<String, String> keys = new Hashtable<>();
                keys.put("name", entry.getKey());
                keys.put("type", typeOf(entry.getValue()));
                final ObjectName name = new ObjectName(domain, keys);
                server.registerMBean(bean, name);
                registered.add(name);
            } catch (Exception e) {
                LOG.warn("Could not register metric {} as an MBean; it will be missing from JMX.",
                        entry.getKey(), e);
            }
        }
        // Deliberately INFO: whether this happened, and for how many metrics, must be readable
        // straight from the log - a reporting layer whose absence is invisible cannot be trusted.
        LOG.info("JMX metric reporting started: {} MBeans registered in domain {}.", registered.size(), domain);
    }

    /** Unregisters everything {@link #start()} registered. Safe to call once, late, or never. */
    void stop() {
        for (ObjectName name : registered) {
            try {
                server.unregisterMBean(name);
            } catch (Exception e) {
                LOG.warn("Could not unregister MBean {}; it may linger until JVM restart.", name, e);
            }
        }
        registered.clear();
    }

    private static DynamicMBean mbeanFor(final Metric metric) {
        if (metric instanceof Meter) {
            return new MeterMBean((Meter) metric);
        }
        if (metric instanceof Gauge) {
            return new GaugeMBean((Gauge<?>) metric);
        }
        return null;
    }

    private static String typeOf(final Metric metric) {
        return metric instanceof Meter ? "meters" : "gauges";
    }

    /** A meter's monotonic count, as the {@code Count} attribute Dropwizard's reporter exposes. */
    private static final class MeterMBean extends SingleAttributeMBean {
        private final Meter meter;

        MeterMBean(final Meter meter) {
            super("Count", Long.class.getName(), "Monotonic count of marked events");
            this.meter = meter;
        }

        @Override
        Object value() {
            return meter.getCount();
        }
    }

    /** A gauge's current value, as the {@code Value} attribute Dropwizard's reporter exposes. */
    private static final class GaugeMBean extends SingleAttributeMBean {
        private final Gauge<?> gauge;

        GaugeMBean(final Gauge<?> gauge) {
            super("Value", Object.class.getName(), "Current value");
            this.gauge = gauge;
        }

        @Override
        Object value() {
            return gauge.getValue();
        }
    }

    /** The one shape every metric here needs: a single read-only attribute, no operations. */
    private abstract static class SingleAttributeMBean implements DynamicMBean {
        private final String attributeName;
        private final MBeanInfo info;

        SingleAttributeMBean(final String attributeName, final String attributeType, final String description) {
            this.attributeName = attributeName;
            this.info = new MBeanInfo(getClass().getName(), description,
                    new MBeanAttributeInfo[]{
                            new MBeanAttributeInfo(attributeName, attributeType, description, true, false, false)},
                    null, null, null);
        }

        abstract Object value();

        @Override
        public Object getAttribute(final String attribute) throws AttributeNotFoundException {
            if (!attributeName.equals(attribute)) {
                throw new AttributeNotFoundException(attribute);
            }
            return value();
        }

        @Override
        public AttributeList getAttributes(final String[] attributes) {
            final AttributeList list = new AttributeList();
            for (String attribute : attributes) {
                if (attributeName.equals(attribute)) {
                    list.add(new Attribute(attributeName, value()));
                }
            }
            return list;
        }

        @Override
        public void setAttribute(final Attribute attribute) throws AttributeNotFoundException {
            throw new AttributeNotFoundException(
                    (attribute == null ? "(null)" : attribute.getName()) + " is read-only");
        }

        @Override
        public AttributeList setAttributes(final AttributeList attributes) {
            return new AttributeList();
        }

        @Override
        public Object invoke(final String actionName, final Object[] params, final String[] signature)
                throws MBeanException {
            throw new MBeanException(new UnsupportedOperationException(actionName), "no operations");
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            return info;
        }
    }
}
