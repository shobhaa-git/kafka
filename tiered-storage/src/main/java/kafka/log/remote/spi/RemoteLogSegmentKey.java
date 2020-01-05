package kafka.log.remote.spi;

import org.apache.kafka.common.TopicPartition;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * The unique key associated to the segment of a topic-partition.
 * This key is universally unique. This means different attempt from the
 * metadata store to generate a metadata for the same log segment will
 * yield a different key.
 */
/* @Immutable */
public final class RemoteLogSegmentKey {
    private final TopicPartition topicPartition;
    private final UUID id; // As an example...

    public RemoteLogSegmentKey(final TopicPartition topicPartition, final UUID id) {
        this.topicPartition = requireNonNull(topicPartition);
        this.id = requireNonNull(id);
    }

    // Accessors as required.

    @Override
    public int hashCode() {
        return Long.valueOf(id.getLeastSignificantBits()).intValue();
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof RemoteLogSegmentKey)) {
            return false;
        }
        return ((RemoteLogSegmentKey) other).id.equals(id);
    }

    @Override
    public String toString() {
        return String.format("%s[%s: %d]", getClass().getSimpleName(), topicPartition, id);
    }
}
