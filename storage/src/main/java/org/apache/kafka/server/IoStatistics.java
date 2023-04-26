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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.kafka.server.LinuxBrokerLogDirHealthMonitor.IOSTATS_TOPIC_NAME;

public abstract class IoStatistics {
    private static final Logger log = LoggerFactory.getLogger(IoStatistics.class);

    public static IoStatistics newIoStatistics(Instant time, String stat) {
        String[] stats = Arrays.stream(stat.split("\\D+"))
            .filter(s -> !s.isEmpty())
            .toArray(String[]::new);

        // https://docs.kernel.org/block/stat.html
        long readsCompleted = Long.parseLong(stats[0]);
        long readTime = Long.parseLong(stats[3]);
        long writesCompleted = Long.parseLong(stats[4]);
        long writeTime = Long.parseLong(stats[7]);
        long queueTime = Long.parseLong(stats[10]);

        return new Snapshot(time, readsCompleted, readTime, writesCompleted, writeTime, queueTime);
    }

    protected final long readsCompleted;
    protected final long readTime;
    protected final long writesCompleted;
    protected final long writeTime;
    protected final long queueTime;

    private static final class Snapshot extends IoStatistics implements Serializable {
        private final Instant time;

        private Snapshot(Instant time, long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
            super(readsCompleted, readTime, writesCompleted, writeTime, queueTime);
            this.time = time;
        }

        @Override
        public IoStatistics delta(IoStatistics origin) {
            if (!(origin instanceof Snapshot)) {
                throw new IllegalStateException("IoStatistics is not a snapshot, cannot compute delta.");
            }
            return new Delta((Snapshot) origin, this);
        }

        @Override
        public double ioQueueSize() {
            throw new IllegalStateException("I/O queue size cannot be defined for an IoStatistics snapshot.");
        }

        @Override
        public ProducerRecord<Long, byte[]> toProducerRecord() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(this);
                oos.flush();
                return new ProducerRecord<>(IOSTATS_TOPIC_NAME, time.toEpochMilli(), baos.toByteArray());

            } catch (IOException e) {
                log.error("Error generating record from IoStatistics", e);
                return new ProducerRecord<>(IOSTATS_TOPIC_NAME, time.toEpochMilli(), "<ERROR>".getBytes(defaultCharset()));
            }
        }
    }

    private static final class Delta extends IoStatistics {
        private final Duration timeSpan;

        private Delta(Snapshot origin, Snapshot stat) {
            super(
                stat.readsCompleted - origin.readsCompleted,
                stat.readTime - origin.readTime,
                stat.writesCompleted - origin.writesCompleted,
                stat.writeTime - origin.writeTime,
                stat.queueTime - origin.queueTime
            );

            this.timeSpan = Duration.between(origin.time, stat.time);
        }

        @Override
        public IoStatistics delta(IoStatistics origin) {
            throw new IllegalStateException("IoStatistics is not a snapshot, cannot compute delta.");
        }

        @Override
        public double ioQueueSize() {
            return (double) queueTime / timeSpan.toMillis();
        }

        @Override
        public ProducerRecord<Long, byte[]> toProducerRecord() {
            throw new IllegalStateException("Delta Statistics are not authorized to be converted to records.");
        }
    }

    protected IoStatistics(long readsCompleted, long readTime, long writesCompleted, long writeTime, long queueTime) {
        this.readsCompleted = readsCompleted;
        this.readTime = readTime;
        this.writesCompleted = writesCompleted;
        this.writeTime = writeTime;
        this.queueTime = queueTime;
    }

    public abstract IoStatistics delta(IoStatistics origin);

    public abstract double ioQueueSize();

    public abstract ProducerRecord<Long, byte[]> toProducerRecord();

    public double readOpsLatency() {
        return (double) readTime / readsCompleted;
    }

    public double writeOpsLatency() {
        return (double) writeTime / writesCompleted;
    }
}
