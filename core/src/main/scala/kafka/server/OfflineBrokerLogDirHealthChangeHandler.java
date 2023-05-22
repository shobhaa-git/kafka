/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;

import org.apache.kafka.server.BrokerLogDirHealth;
import org.apache.kafka.server.BrokerLogDirHealthChangeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineBrokerLogDirHealthChangeHandler implements BrokerLogDirHealthChangeHandler {
    private static final Logger log = LoggerFactory.getLogger(OfflineBrokerLogDirHealthChangeHandler.class);

    private LogDirFailureChannel logDirFailureChannel;

    public OfflineBrokerLogDirHealthChangeHandler(final LogDirFailureChannel logDirFailureChannel) {
        this.logDirFailureChannel = logDirFailureChannel;
    }

    @Override
    public void onBrokerLogDirHealthChanged(String logDirectory, BrokerLogDirHealth health) {
        if(health != BrokerLogDirHealth.Healthy) {
            // Mark the log directory as degraded. This will mark all the partitions offline but still keep the broker running.
            log.warn("Broker log directory is unhealthy {} {}", logDirectory, health);
            logDirFailureChannel.maybeAddOfflineLogDir(logDirectory, OfflineLogDirState.DEGRADED, () -> "Broker log directory is unhealthy");
        }
    }

}

