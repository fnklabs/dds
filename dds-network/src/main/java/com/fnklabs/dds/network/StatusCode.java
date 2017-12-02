package com.fnklabs.dds.network;

/**
 * Response status code
 */
public enum StatusCode {
    /**
     * BaseMessage was successfully processed
     */
    OK(0),

    /**
     * Invalid message format
     */
    BAD_FORMAT(200),

    /**
     * Unknown or unsupported message api
     */
    UNKNOWN_API_VERSION(210),

    /**
     * Server is busy and can't process request
     */
    SERVER_IS_BUSY(500),

    SERVER_IS_NOT_AVAILABLE_FOR_OPERATIONS(510),

    /**
     * Processing timeout
     */
    TIMEOUT(600),

    /**
     * Can't pack message
     */
    CANT_PACK_MESSAGE(700),

    /**
     * Unexpected exception
     */
    UNKNOWN(100),;

    private int value;

    StatusCode(int value) {
        this.value = value;
    }

    public static StatusCode valueOf(int value) {
        for (StatusCode operationType : values()) {
            if (operationType.value() == value) {
                return operationType;
            }
        }

        throw new IllegalArgumentException(String.format("Invalid status code value: %d", value));
    }

    public int value() {
        return value;
    }
}
