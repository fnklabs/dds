package com.fnklabs.dds;

import com.codahale.metrics.*;

import java.util.concurrent.TimeUnit;

public class Metrics {
    private static final MetricRegistry metricRegistry = new MetricRegistry();

    public static final Slf4jReporter reporter = Slf4jReporter
            .forRegistry(metricRegistry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    public static Timer getTimer(Type type) {
        return metricRegistry.timer(type.toString());
    }

    public static Counter getCounter(Type counter) {
        return metricRegistry.counter(counter.toString());
    }

    public static Histogram getHistogram(Type type) {
        return metricRegistry.histogram(type.toString());
    }

    public static Meter getMeter(Type netSendBytes) {
        return metricRegistry.meter(netSendBytes.toString());
    }

    @Deprecated
    public static Timer getWriteTimer() {
        return getTimer(Type.WRITE_OPERATIONS);
    }

    @Deprecated
    public static Timer getReadTimer() {
        return getTimer(Type.READ_OPERATIONS);
    }

    public enum Type {
        /**
         * IO operations
         */
        WRITE_OPERATIONS,
        READ_OPERATIONS,
        SEND_MESSAGES,
        RECEIVED_MESSAGES,
        RESPONSE,

        /**
         * Net IO operations
         */

        NET_CLIENT_RETRIEVED_MESSAGES,
//        NET_CLIENT_SEND_MESSAGES,
        NET_CLIENT_SUCCESS_SEND_MESSAGES,
        NET_CLIENT_FAILED_SEND_MESSAGES,
        NET_CLIENT_SEND_MESSAGES,
        NET_CLIENT_PACK_MSG,
        NET_SERIALIZE_OBJECT,
        NET_UNSERIALIZE_OBJECT,

        NET_SERVER_PROCESS_SELECTOR,
        NET_SERVER_SEND_MESSAGES,
        NET_SERVER_PROCESS_REQUEST,

        EXCEPTIONS,

        /**
         * DDS and Chunk Operations
         */
        DDS_MAP_OPERATION,
        CHUNK_MAP_OPERATION,

        DDS_FOREACH_OPERATION,
        CHUNK_FOREACH_OPERATION,

        DDS_REDUCE_OPERATION,
        CHUNK_REDUCE_OPERATION,

        COMPLETED_JOBS,
        NET_MESSAGE_TRANSFORM, NET_MESSAGE_UNPACK, NET_WRITE_BYTES, NET_SEND_BYTES, NET_PACKET_SPLIT, NET_SELECTOR_EVENTS, NET_CLIENT_RETRIEVED_PACKETS, NET_CLIENT_PROCESSED_EVENTS, FAILED_JOBS
    }

    static {
        reporter.start(5, TimeUnit.SECONDS);
    }

}
