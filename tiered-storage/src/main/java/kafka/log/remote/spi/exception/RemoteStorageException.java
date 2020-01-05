package kafka.log.remote.spi.exception;

public abstract class RemoteStorageException extends Exception {

    private final boolean isRetryable;

    public RemoteStorageException(boolean isRetryable) {
        this.isRetryable = isRetryable;
    }
}
