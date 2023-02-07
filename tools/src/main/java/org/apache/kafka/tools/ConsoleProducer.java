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
package org.apache.kafka.tools;

import joptsimple.OptionException;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.ToolsUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Pattern;

import static java.util.Arrays.stream;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.utils.Utils.loadProps;
import static org.apache.kafka.server.util.CommandLineUtils.maybeMergeOptions;
import static org.apache.kafka.server.util.CommandLineUtils.parseKeyValueArgs;

public class ConsoleProducer {
    public static void main(String[] args) throws Exception {
        try {
            ConsoleProducerConfig config = new ConsoleProducerConfig(args);
            MessageReader reader = (MessageReader) Class.forName(config.readerClass()).getDeclaredConstructor().newInstance();
            reader.init(System.in, config.getReaderProps());

            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config.getProducerProps());
            Exit.addShutdownHook("producer-shutdown-hook", producer::close);

            ProducerRecord<byte[], byte[]> record;
            do {
                record = reader.readMessage();
                if (record != null) {
                    send(producer, record, config.sync());
                }
            } while (record != null);

        } catch (OptionException e) {
            System.err.println(e.getMessage());
            Exit.exit(1);

        } catch (Exception e) {
            e.printStackTrace();
            Exit.exit(1);
        }
        Exit.exit(0);
    }

    private static void send(KafkaProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record, boolean sync) throws Exception {
        if (sync) {
            producer.send(record).get();
        } else {
            producer.send(record, new ErrorLoggingCallback(record.topic(), record.key(), record.value(), false));
        }
    }

    public static class ConsoleProducerConfig extends CommandDefaultOptions {
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> brokerListOpt;
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<Void> syncOpt;
        private final OptionSpec<String> compressionCodecOpt;
        private final OptionSpec<Integer> batchSizeOpt;
        private final OptionSpec<Integer> messageSendMaxRetriesOpt;
        private final OptionSpec<Long> retryBackoffMsOpt;
        private final OptionSpec<Long> sendTimeoutOpt;
        private final OptionSpec<String> requestRequiredAcksOpt;
        private final OptionSpec<Integer> requestTimeoutMsOpt;
        private final OptionSpec<Long> metadataExpiryMsOpt;
        private final OptionSpec<Long> maxBlockMsOpt;
        private final OptionSpec<Long> maxMemoryBytesOpt;
        private final OptionSpec<Integer> maxPartitionMemoryBytesOpt;
        private final OptionSpec<String> messageReaderOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        private final OptionSpec<String> propertyOpt;
        private final OptionSpec<String> readerConfigOpt;
        private final OptionSpec<String> producerPropertyOpt;
        private final OptionSpec<String> producerConfigOpt;

        public ConsoleProducerConfig(String[] args) {
            super(args);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
                    .withRequiredArg()
                    .describedAs("topic")
                    .ofType(String.class);
            brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified.  The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                    .withRequiredArg()
                    .describedAs("broker-list")
                    .ofType(String.class);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to. The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                    .requiredUnless("broker-list")
                    .withRequiredArg()
                    .describedAs("server to connect to")
                    .ofType(String.class);
            syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.");
            compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or 'zstd'." +
                            "If specified without value, then it defaults to 'gzip'")
                    .withOptionalArg()
                    .describedAs("compression-codec")
                    .ofType(String.class);
            batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously. " +
                            "please note that this option will be replaced if max-partition-memory-bytes is also set")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(16 * 1024);
            messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, " +
                            "and being unavailable transiently is just one of them. This property specifies the number of retries before the producer give up and drop this message. " +
                            "This is the option to control `retries` in producer configs.")
                    .withRequiredArg()
                    .ofType(Integer.class)
                    .defaultsTo(3);
            retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. " +
                            "Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata. " +
                            "This is the option to control `retry.backoff.ms` in producer configs.")
                    .withRequiredArg()
                    .ofType(Long.class)
                    .defaultsTo(100L);
            sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
                            " a message will queue awaiting sufficient batch size. The value is given in ms. " +
                            "This is the option to control `linger.ms` in producer configs.")
                    .withRequiredArg()
                    .describedAs("timeout_ms")
                    .ofType(Long.class)
                    .defaultsTo(1000L);
            requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required `acks` of the producer requests")
                    .withRequiredArg()
                    .describedAs("request required acks")
                    .ofType(String.class)
                    .defaultsTo("-1");
            requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero.")
                    .withRequiredArg()
                    .describedAs("request timeout ms")
                    .ofType(Integer.class)
                    .defaultsTo(1500);
            metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
                            "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes. " +
                            "This is the option to control `metadata.max.age.ms` in producer configs.")
                    .withRequiredArg()
                    .describedAs("metadata expiration interval")
                    .ofType(Long.class)
                    .defaultsTo(5 * 60 * 1000L);
            maxBlockMsOpt = parser.accepts("max-block-ms",
                            "The max time that the producer will block for during a send request.")
                    .withRequiredArg()
                    .describedAs("max block on send")
                    .ofType(Long.class)
                    .defaultsTo(60 * 1000L);
            maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
                            "The total memory used by the producer to buffer records waiting to be sent to the server. " +
                            "This is the option to control `buffer.memory` in producer configs.")
                    .withRequiredArg()
                    .describedAs("total memory in bytes")
                    .ofType(Long.class)
                    .defaultsTo(32 * 1024 * 1024L);
            maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
                            "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
                            "will attempt to optimistically group them together until this size is reached. " +
                            "This is the option to control `batch.size` in producer configs.")
                    .withRequiredArg()
                    .describedAs("memory in bytes per partition")
                    .ofType(Integer.class)
                    .defaultsTo(16 * 1024);
            messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
                            "By default each line is read as a separate message.")
                    .withRequiredArg()
                    .describedAs("reader_class")
                    .ofType(String.class)
                    .defaultsTo(LineMessageReader.class.getName());
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size. " +
                            "This is the option to control `send.buffer.bytes` in producer configs.")
                    .withRequiredArg()
                    .describedAs("size")
                    .ofType(Integer.class)
                    .defaultsTo(1024 * 100);
            propertyOpt = parser.accepts("property",
                            "A mechanism to pass user-defined properties in the form key=value to the message reader. This allows custom configuration for a user-defined message reader." +
                            "Default properties include:" +
                            "\n parse.key=false" +
                            "\n parse.headers=false" +
                            "\n ignore.error=false" +
                            "\n key.separator=\\t" +
                            "\n headers.delimiter=\\t" +
                            "\n headers.separator=," +
                            "\n headers.key.separator=:" +
                            "\n null.marker=   When set, any fields (key, value and headers) equal to this will be replaced by null" +
                            "\nDefault parsing pattern when:" +
                            "\n parse.headers=true and parse.key=true:" +
                            "\n  \"h1:v1,h2:v2...\\tkey\\tvalue\"" +
                            "\n parse.key=true:" +
                            "\n  \"key\\tvalue\"" +
                            "\n parse.headers=true:" +
                            "\n  \"h1:v1,h2:v2...\\tvalue\"")
                    .withRequiredArg()
                    .describedAs("prop")
                    .ofType(String.class);
            readerConfigOpt = parser.accepts("reader-config", "Config properties file for the message reader. Note that " + propertyOpt + " takes precedence over this config.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);
            producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
                    .withRequiredArg()
                    .describedAs("producer_prop")
                    .ofType(String.class);
            producerConfigOpt = parser.accepts("producer.config", "Producer config properties file. Note that " + producerPropertyOpt + " takes precedence over this config.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);

            try {
                options = parser.parse(args);

            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from standard input and publish it to Kafka.");
            CommandLineUtils.checkRequiredArgs(parser, options, topicOpt);

            ToolsUtils.validatePortOrDie(parser, brokerHostsAndPorts());
        }

        String topic() {
            return options.valueOf(topicOpt);
        }

        String bootstrapServer() {
            return options.valueOf(bootstrapServerOpt);
        }

        String brokerList() {
            return options.valueOf(brokerListOpt);
        }

        String brokerHostsAndPorts() {
            return options.has(bootstrapServerOpt) ? bootstrapServer() : brokerList();
        }

        boolean sync() {
            return options.has(syncOpt);
        }

        String compressionCodecOptionValue() {
            return options.valueOf(compressionCodecOpt);
        }

        String compressionCodec() {
            if (options.has(compressionCodecOpt)) {
                String codecOptValue = compressionCodecOptionValue();
                // Defaults to gzip if no value is provided.
                return codecOptValue == null || codecOptValue.isEmpty() ? CompressionType.GZIP.name : codecOptValue;
            }

            return CompressionType.NONE.name;
        }

        String readerClass() {
            return options.valueOf(messageReaderOpt);
        }

        Properties getReaderProps() throws IOException {
            Properties properties = new Properties();

            if (options.has(readerConfigOpt)) {
                properties.putAll(loadProps(options.valueOf(readerConfigOpt)));
            }

            properties.put("topic", topic());
            properties.putAll(parseKeyValueArgs(options.valuesOf(propertyOpt)));
            return properties;
        }

        Properties getProducerProps() throws IOException {
            Properties properties = new Properties();

            if (options.has(producerConfigOpt)) {
                properties.putAll(loadProps(options.valueOf(producerConfigOpt)));
            }

            properties.putAll(parseKeyValueArgs(options.valuesOf(producerPropertyOpt)));
            properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer() != null ? bootstrapServer() : brokerList());
            properties.put(COMPRESSION_TYPE_CONFIG, compressionCodec());
            properties.putIfAbsent(CLIENT_ID_CONFIG, "console-producer");
            properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            maybeMergeOptions(properties, LINGER_MS_CONFIG, options, sendTimeoutOpt);
            maybeMergeOptions(properties, ACKS_CONFIG, options, requestRequiredAcksOpt);
            maybeMergeOptions(properties, REQUEST_TIMEOUT_MS_CONFIG, options, requestTimeoutMsOpt);
            maybeMergeOptions(properties, RETRIES_CONFIG, options, messageSendMaxRetriesOpt);
            maybeMergeOptions(properties, RETRY_BACKOFF_MS_CONFIG, options, retryBackoffMsOpt);
            maybeMergeOptions(properties, SEND_BUFFER_CONFIG, options, socketBufferSizeOpt);
            maybeMergeOptions(properties, BUFFER_MEMORY_CONFIG, options, maxMemoryBytesOpt);
            // We currently have 2 options to set the batch.size value. We'll deprecate/remove one of them in KIP-717.
            maybeMergeOptions(properties, BATCH_SIZE_CONFIG, options, batchSizeOpt);
            maybeMergeOptions(properties, BATCH_SIZE_CONFIG, options, maxPartitionMemoryBytesOpt);
            maybeMergeOptions(properties, METADATA_MAX_AGE_CONFIG, options, metadataExpiryMsOpt);
            maybeMergeOptions(properties, MAX_BLOCK_MS_CONFIG, options, maxBlockMsOpt);

            return properties;
        }
    }

    static final class LineMessageReader implements MessageReader {
        private String topic;
        private BufferedReader reader;
        private boolean parseKey;
        private String keySeparator = "\\t";
        private boolean parseHeaders;
        private String headersDelimiter = "\\t";
        private String headersSeparator = ",";
        private String headersKeySeparator = ":";
        private boolean ignoreError;
        private int lineNumber;
        private boolean printPrompt = System.console() != null;
        private Pattern headersSeparatorPattern;
        private String nullMarker;

        @Override
        public void init(InputStream inputStream, Properties props) {
            topic = props.getProperty("topic");
            if (props.containsKey("parse.key"))
                parseKey = props.getProperty("parse.key").trim().equalsIgnoreCase("true");
            if (props.containsKey("key.separator"))
                keySeparator = props.getProperty("key.separator");
            if (props.containsKey("parse.headers"))
                parseHeaders = props.getProperty("parse.headers").trim().equalsIgnoreCase("true");
            if (props.containsKey("headers.delimiter"))
                headersDelimiter = props.getProperty("headers.delimiter");
            if (props.containsKey("headers.separator"))
                headersSeparator = props.getProperty("headers.separator");
            headersSeparatorPattern = Pattern.compile(headersSeparator);
            if (props.containsKey("headers.key.separator"))
                headersKeySeparator = props.getProperty("headers.key.separator");
            if (props.containsKey("ignore.error"))
                ignoreError = props.getProperty("ignore.error").trim().equalsIgnoreCase("true");
            if (headersDelimiter.equals(headersSeparator))
                throw new KafkaException("headers.delimiter and headers.separator may not be equal");
            if (headersDelimiter.equals(headersKeySeparator))
                throw new KafkaException("headers.delimiter and headers.key.separator may not be equal");
            if (headersSeparator.equals(headersKeySeparator))
                throw new KafkaException("headers.separator and headers.key.separator may not be equal");
            if (props.containsKey("null.marker"))
                nullMarker = props.getProperty("null.marker");
            if (keySeparator.equals(nullMarker))
                throw new KafkaException("null.marker and key.separator may not be equal");
            if (headersSeparator.equals(nullMarker))
                throw new KafkaException("null.marker and headers.separator may not be equal");
            if (headersDelimiter.equals(nullMarker))
                throw new KafkaException("null.marker and headers.delimiter may not be equal");
            if (headersKeySeparator.equals(nullMarker))
                throw new KafkaException("null.marker and headers.key.separator may not be equal");

            reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        }

        @Override
        public ProducerRecord<byte[], byte[]> readMessage() {
            ++lineNumber;
            if (printPrompt) {
                System.out.print(">");
            }

            String line;
            try {
                line = reader.readLine();

            } catch (IOException e) {
                throw new KafkaException(e);
            }

            if (line == null) {
                return null;
            }

            String headers = parse(parseHeaders, line, 0, headersDelimiter, "headers delimiter");
            int headerOffset = headers == null ? 0 : headers.length() + headersDelimiter.length();

            String key = parse(parseKey, line, headerOffset, keySeparator, "key separator");
            int keyOffset = key == null ? 0 : key.length() + keySeparator.length();

            String value = line.substring(headerOffset + keyOffset);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    topic,
                    key != null && !key.equals(nullMarker) ? key.getBytes(StandardCharsets.UTF_8) : null,
                    value != null && !value.equals(nullMarker) ? value.getBytes(StandardCharsets.UTF_8) : null
            );

            if (headers != null && !headers.equals(nullMarker)) {
                stream(splitHeaders(headers)).forEach(header -> record.headers().add(header.key, header.value));
            }

            return record;
        }

        private String parse(boolean enabled, String line, int startIndex, String demarcation, String demarcationName) {
            if (!enabled) {
                return null;
            }
            int index = line.indexOf(demarcation, startIndex);
            if (index == -1) {
                if (ignoreError) {
                    return null;
                }
                throw new KafkaException("No " + demarcationName + " found on line number " + lineNumber + ": " + line);
            }
            return line.substring(startIndex, index);
        }

        private Header[] splitHeaders(String headers) {
            return stream(headersSeparatorPattern.split(headers))
                    .map(pair -> {
                        int i = pair.indexOf(headersKeySeparator);
                        if (i == -1) {
                            if (ignoreError) {
                                return null;
                            }
                            throw new KafkaException("No header key separator found in pair '" + pair + "' on line number " + lineNumber);
                        }

                        String headerKey = pair.substring(0, i);
                        if (headerKey.equals(nullMarker)) {
                            throw new KafkaException("Header keys should not be equal to the null marker " + nullMarker + "' as they can't be null");
                        }

                        String value = pair.substring(i + headersKeySeparator.length());
                        byte[] headerValue = value.equals(nullMarker) ? null : value.getBytes(StandardCharsets.UTF_8);
                        return new Header(headerKey, headerValue);

                    }).toArray(Header[]::new);
        }

        // VisibleForTesting
        String keySeparator() {
            return keySeparator;
        }

        // VisibleForTesting
        boolean parseKey() {
            return parseKey;
        }

        boolean parseHeaders() {
            return parseHeaders;
        }
    }

    private static class Header {
        private final String key;
        private final byte[] value;

        Header(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
