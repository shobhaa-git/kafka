package kafka.log.remote.spi.exception;

public class RecordsKeyNotFoundException extends RemoteStorageException {

    /**
     * Eventually consistent storage for read-after-write may indicate operation can be successful on retry.
     *
     * @param isRetryable
     */
    public RecordsKeyNotFoundException(boolean isRetryable) {
        super(isRetryable);
    }
}
