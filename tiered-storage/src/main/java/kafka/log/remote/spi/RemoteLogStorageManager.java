package kafka.log.remote.spi;

import kafka.log.remote.spi.exception.RecordsKeyNotFoundException;
import kafka.log.remote.spi.exception.RemoteStorageAvailabilityException;
import kafka.log.remote.spi.exception.RemoteStorageException;

import java.io.InputStream;

/**
 * For transfer of log segments.
 *
 * The idea here is for all Kafka-related concepts to be fully abstracted from the
 * {@code RemoteLogStorageManager} and to separate all control structure
 * (metadata, indexes, etc.) from the remote storage itself. That way, metadata and
 * indexes management is completely encapsulated in Apache Kafka, and implementors
 * of remote storage would only have to deal with as little as domain-specific
 * knowledge as possible.
 *
 * This also makes it easier to reason about consistency models of remote storages.
 * Implementation of {@code RemoteLogStorageManager} can be eventually consistent. This of
 * course exposes to potentially absent remote data but the metadata itself would
 * be consistent at all times.
 *
 * In order to ensure that the right records are read, the {@code RemoteLogSegmentId} is
 * universally unique, and all uploads of blob of data (be it from the same
 * topic-partition for the same offsets) will be assigned a different key. This
 * allows to deal with case of read-after-write-after-delete where for instance
 * S3 is only eventually consistent. Past data cannot be read. Uniqueness of the
 * key is managed by Apache Kafka.
 *
 * This is for instance the approach adopted by Apache Pulsar when a ledger is
 * offloaded to a tiered storage system.
 *
 */
public interface RemoteLogStorageManager {

    RemoteLogSegmentContext copyLogSegment(RemoteLogSegmentId id, LogSegmentData data) throws RemoteStorageException;

    InputStream fetchLogSegmentData(RemoteLogSegmentId id, boolean indexesOnly) throws RemoteStorageException;

    InputStream fetchOffsetIndex(RemoteLogSegmentId id) throws RemoteStorageException;

    InputStream fetchTimestampIndex(RemoteLogSegmentId id) throws RemoteStorageException;

    boolean deleteLogSegment(RemoteLogSegmentId id) throws RemoteStorageException;

}
