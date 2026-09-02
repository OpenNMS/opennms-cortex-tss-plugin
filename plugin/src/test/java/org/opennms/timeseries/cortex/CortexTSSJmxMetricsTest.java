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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

/**
 * The plugin's metrics must be reachable through the JMX machinery OpenNMS already scrapes its
 * own JVM with (the OpenNMS-JVM service / Jsr160 collector) - not only through the interactive
 * {@code opennms-cortex:stats} Karaf command - or samplesLost cannot be trended or alerted on.
 */
public class CortexTSSJmxMetricsTest {

    @Test
    public void mirrorsItsMetricsAsMBeansAndUnregistersThemOnDestroy() throws Exception {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName samplesLost = new ObjectName(CortexTSS.JMX_DOMAIN + ":name=samplesLost,type=meters");
        final ObjectName bufferedSamples = new ObjectName(CortexTSS.JMX_DOMAIN + ":name=batch.bufferedSamples,type=gauges");
        // The reporter cannot re-register a name another instance left behind, so a leak here
        // would make the assertions below read a dead instance's meters. Fail loudly instead.
        assertFalse("another CortexTSS instance leaked its MBeans; fix that test's teardown first",
                server.isRegistered(samplesLost));

        final CortexTSS tss = new CortexTSS(CortexTSSConfig.builder().batchingEnabled(true).build(),
                new KVStoreMock());
        try {
            assertEquals("a meter must surface its count", 0L, server.getAttribute(samplesLost, "Count"));
            assertEquals("a gauge must surface its value", 0, server.getAttribute(bufferedSamples, "Value"));
        } finally {
            tss.destroy();
        }
        assertFalse("destroy() must unregister the MBeans, or a reloaded bundle could never publish its own",
                server.isRegistered(samplesLost));
        assertFalse(server.isRegistered(bufferedSamples));
    }

}
