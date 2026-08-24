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

/**
 * A write failure that is worth retrying with the same payload: HTTP 429, 5xx, or an I/O error
 * before a response was read. Anything else (4xx) means the payload itself is unacceptable and
 * retrying it can never succeed.
 */
public class RetryableWriteException extends StorageException {

    private static final long serialVersionUID = 1L;

    public RetryableWriteException(final String message) {
        super(message);
    }

    public RetryableWriteException(final String message, final Throwable cause) {
        // StorageException(String, Throwable) discards the message, so attach the cause manually.
        super(message);
        initCause(cause);
    }
}
