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

import java.time.Instant;
import java.util.function.Predicate;

public class TimeSeries {
    private final Instant[] times;
    private final double[] data;
    private int cursor;

    public TimeSeries(int length) {
        this.times = new Instant[length];
        this.data = new double[length];
    }

    public void add(Instant time, double value) {
        times[cursor] = time;
        data[cursor] = value;
        cursor = (cursor + 1) % data.length;
    }

    public Window newWindowFromLastSample(int size) {
        int start = (data.length + cursor - 1 - size) % data.length;
        int end = (data.length + cursor - 1) % data.length;
        return new Window(start, end);
    }

    public final class Window {
        private final int start;
        private final int end;

        private Window(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int count(Predicate<Double> filter) {
            int c = 0;
            for (int i = start; i <= end; ++i) {
                if (filter.test(data[i])) {
                    ++c;
                }
            }
            return c;
        }
    }
}
