/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class AggregatedIoStatistics {
    private static final Logger log = LoggerFactory.getLogger(AggregatedIoStatistics.class);

    // 5 seconds * 360 = 30 minutes.
    private static final int DATAPOINT_COUNT = 360;

    private final TimeSeries readLatency;
    private final TimeSeries writeLatency;
    private IoStatistics last;

    public AggregatedIoStatistics() {
        this.readLatency = new TimeSeries(DATAPOINT_COUNT);
        this.writeLatency = new TimeSeries(DATAPOINT_COUNT);
    }

    public void push(IoStatistics snapshot) {
        IoStatistics origin = last;
        last = snapshot;

        if (origin != null) {
            IoStatistics delta = snapshot.delta(origin);
            Instant now = Instant.now();

            log.info("avg-read-latency: " + delta.readOpsLatency() + " ms/ops");
            log.info("avg-write-latency: " + delta.writeOpsLatency() + " ms/ops");
            log.info("io-queue-size: " + delta.ioQueueSize());

            readLatency.add(now, delta.readOpsLatency());
            writeLatency.add(now, delta.writeOpsLatency());
        }
    }

    public TimeSeries readLatency() {
        return readLatency;
    }

    public TimeSeries writeLatency() {
        return writeLatency;
    }

}
