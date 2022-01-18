package org.opennms.timeseries.cortex;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;

public interface TagStorage {
    void storeTags(Sample sample, String tenantID);
    Metric retrieveTags(Metric metric, String tenantID, long endTimeEpochMillis);
}
