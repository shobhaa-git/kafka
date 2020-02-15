package kafka.log.remote.spi;

import java.util.Arrays;

/* @Immutable */
public final class RemoteLogSegmentContext {
    private final byte[] payload;

    public RemoteLogSegmentContext(final byte[] ctxPayload) {
        this.payload = Arrays.copyOf(ctxPayload, ctxPayload.length);
    }

    public byte[] asBytes() {
        return Arrays.copyOf(payload, payload.length);
    }

}
