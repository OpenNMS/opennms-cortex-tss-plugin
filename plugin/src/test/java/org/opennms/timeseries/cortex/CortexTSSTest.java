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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.opennms.timeseries.cortex.CortexTSS.LABEL_NAME_PATTERN;
import static org.opennms.timeseries.cortex.CortexTSS.METRIC_NAME_PATTERN;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;
import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTimeSeriesFetchRequest;

public class CortexTSSTest {

    @Test
    public void canSanitizeMetricName() {
        String metricName = "name=jmx-minion_resourceId=response:127.0.0.1:jmx-minion";
        assertThat(METRIC_NAME_PATTERN.matcher(metricName).matches(), equalTo(false));

        String sanitizedMetricName = CortexTSS.sanitizeMetricName(metricName);
        assertThat(METRIC_NAME_PATTERN.matcher(sanitizedMetricName).matches(), equalTo(true));
        assertThat(sanitizedMetricName, equalTo("name_jmx_minion_resourceId_response:127_0_0_1:jmx_minion"));
    }

    @Test
    public void canSanitizeLabelName() {
        String labelName = "SSH/127.0.0.1";
        assertThat(LABEL_NAME_PATTERN.matcher(labelName).matches(), equalTo(false));

        String sanitizedLabelName = CortexTSS.sanitizeLabelName(labelName);
        assertThat(METRIC_NAME_PATTERN.matcher(sanitizedLabelName).matches(), equalTo(true));
        assertThat(sanitizedLabelName, equalTo("SSH_127_0_0_1"));
    }

    @Test
    public void shouldDetermineDurationCorrectly() {

        assertEquals(1, CortexTSS.determineStepInSeconds(request(1, 0)));
        assertEquals(1, CortexTSS.determineStepInSeconds(request(1000, 0)));
        assertEquals(2, CortexTSS.determineStepInSeconds(request(1001, 0)));
        assertEquals(2, CortexTSS.determineStepInSeconds(request(2000, 0)));
        assertEquals(3, CortexTSS.determineStepInSeconds(request(2001, 0)));

        // no calculation expected since the step is > 0:
        assertEquals(1, CortexTSS.determineStepInSeconds(request(2001, 1)));
    }

    private TimeSeriesFetchRequest request(final long end, final long step) {
        return ImmutableTimeSeriesFetchRequest.builder()
                .start(Instant.ofEpochSecond(0))
                .end(Instant.ofEpochSecond(end))
                .aggregation(Aggregation.NONE)
                .step(Duration.ofSeconds(step))
                .metric(ImmutableMetric.builder().intrinsicTag("aa", "bb").build())
                .build();
    }

}
