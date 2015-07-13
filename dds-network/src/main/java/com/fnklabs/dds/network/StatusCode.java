package com.fnklabs.dds.network;

/**
 * Response status code
 */
public enum StatusCode {
    OK(0),

    BAD_FORMAT(200),
    UNKNOWN_OPERATION_TYPE(210),

    SERVER_IS_BUSY(500),

    TIMEOUT(600),

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
