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

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LinuxBrokerLogDirHealthMonitor implements BrokerLogDirHealthMonitor {
    private static final Logger log = LoggerFactory.getLogger(LinuxBrokerLogDirHealthMonitor.class);

    private static final String PATH = "/sys/block/%s/stat";
    private static final long SAMPLING_PERIOD_SEC = 1;

    private final List<BrokerLogDirHealthChangeHandler> handlers = new CopyOnWriteArrayList<>();
    private final DiskHealthAnalyzer analyzer = new DiskHealthAnalyzer();
    private final AggregatedIoStatistics statistics = new AggregatedIoStatistics();
    private BrokerLogDirHealth currentHealth;

    private volatile ScheduledExecutorService executor;

    @Override
    public void configure(Map<String, ?> configs) {
        executor = newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(() -> readStat(), SAMPLING_PERIOD_SEC, SAMPLING_PERIOD_SEC, SECONDS);
    }

    @Override
    public void close()  {
        executor.shutdownNow();
    }

    @Override
    public void register(BrokerLogDirHealthChangeHandler handler) {
        handlers.add(handler);
    }

    private void readStat() {
        try {
            String path = String.format(PATH, "nvme0n1");
            String stat = new String(Files.readAllBytes(Paths.get(path)), Charset.defaultCharset());

            IoStatistics snapshot = IoStatistics.newIoStatistics(Instant.now(), stat);
            statistics.push(snapshot);

            BrokerLogDirHealth health = analyzer.analyze(statistics);
            if (currentHealth != health) {
                currentHealth = health;
                handlers.forEach(handler -> handler.onBrokerLogDirHealthChanged("", currentHealth));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
