package kafka.log.remote.spi;

import java.nio.ByteBuffer;

/* @Immutable */
public final class RemoteLogSegmentMetadata {

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Start offset of this segment.
     */
    private final long startOffset;

    /**
     * End offset of this segment.
     */
    private final long endOffset;

    /**
     * leader epoch of the broker.
     */
    private final int leaderEpoch;



    public ByteBuffer serialize() {
        return null;
    }

    public static final RemoteLogSegmentMetadata deserialize(final ByteBuffer buffer) {
        return null;
    }


}
