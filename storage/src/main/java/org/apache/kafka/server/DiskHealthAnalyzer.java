package org.apache.kafka.server;

public class DiskHealthAnalyzer {

    public BrokerLogDirHealth analyze(AggregatedIoStatistics statistics) {
        boolean highReadLatency = statistics.readLatency()
            .newWindowFromLastSample(12)
            .count(x -> x >= 100) > 10;

        boolean highWriteLatency = statistics.writeLatency()
            .newWindowFromLastSample(12)
            .count(x -> x >= 100) > 10;

        if (highReadLatency || highWriteLatency) {
            return BrokerLogDirHealth.Slow;
        }

        return BrokerLogDirHealth.Healthy;
    }

}
