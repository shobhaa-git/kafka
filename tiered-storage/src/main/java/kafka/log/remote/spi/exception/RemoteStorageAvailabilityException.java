package kafka.log.remote.spi.exception;

public class RemoteStorageAvailabilityException extends RemoteStorageException {

    /**
     * A
     * @param isRetryable
     */
    public RemoteStorageAvailabilityException(boolean isRetryable) {
        super(isRetryable);
    }
}
