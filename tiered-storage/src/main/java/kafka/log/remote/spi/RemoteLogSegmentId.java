package kafka.log.remote.spi;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

/**
 * The unique key associated to the segment of a topic-partition.
 * This key is universally unique. This means different attempt from the
 * metadata store to generate a metadata for the same log segment will
 * yield a different key.
 */
/* @Immutable */
public final class RemoteLogSegmentId {
    private static final Field.UUID REMOTE_ID = new Field.UUID("id", "The remote log segment ID.");
    private static final Schema SCHEMA = new Schema(TOPIC_NAME, PARTITION_ID, REMOTE_ID);

    private final TopicPartition topicPartition;
    private final UUID id;

    public RemoteLogSegmentId(final TopicPartition topicPartition, final UUID id) {
        this.topicPartition = requireNonNull(topicPartition);
        this.id = requireNonNull(id);
    }

    public UUID getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(id.getLeastSignificantBits()).intValue();
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof RemoteLogSegmentId)) {
            return false;
        }
        return ((RemoteLogSegmentId) other).id.equals(id);
    }

    @Override
    public String toString() {
        return String.format("%s[%s: %d]", getClass().getSimpleName(), topicPartition, id);
    }

    public ByteBuffer serialize() {
        final Struct struct = new Struct(SCHEMA);
        struct.set(TOPIC_NAME, topicPartition.topic());
        struct.set(PARTITION_ID, topicPartition.partition());
        struct.set(REMOTE_ID, id);

        final ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);

        return buffer;
    }

    public static final RemoteLogSegmentId deserialize(final ByteBuffer buffer) {
        final Struct struct = SCHEMA.read(buffer);
        final TopicPartition topicPartition = new TopicPartition(struct.get(TOPIC_NAME), struct.get(PARTITION_ID));
        final UUID id = struct.get(REMOTE_ID);

        return new RemoteLogSegmentId(topicPartition, id);
    }
}
