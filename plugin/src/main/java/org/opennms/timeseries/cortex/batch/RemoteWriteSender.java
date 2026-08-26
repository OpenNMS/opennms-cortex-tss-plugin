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
package org.opennms.timeseries.cortex.batch;

import org.opennms.integration.api.v1.timeseries.StorageException;

import prometheus.PrometheusRemote;

/**
 * Delivers one assembled remote-write request to the backend, synchronously: when this returns
 * normally the backend has acknowledged the batch. {@link ShardedWriteBatcher} relies on that to
 * keep at most one request per shard in flight, which is what guarantees per-series ordering.
 */
@FunctionalInterface
public interface RemoteWriteSender {

    /**
     * @param writeRequest the batch to deliver
     * @param organizationId tenant for the X-Scope-OrgID header; null or blank means no header
     * @throws RetryableWriteException when the same payload may succeed later (429, 5xx, I/O error)
     * @throws StorageException when the payload was rejected and must not be retried
     */
    void send(PrometheusRemote.WriteRequest writeRequest, String organizationId) throws StorageException;
}
