package com.fnklabs.dds.network;

public enum ApiVersion {
    VERSION_1(1);

    public static final ApiVersion CURRENT = ApiVersion.VERSION_1;

    public final int MAX_MESSAGE_SIZE = 1 * 1024 * 1024; // 1mb

    private final int version;

    ApiVersion(int version) {
        this.version = version;
    }

    public static ApiVersion valueOf(int value) {
        for (ApiVersion version : values()) {
            if (version.getVersion() == value) {
                return version;
            }
        }

        throw new IllegalArgumentException(String.format("Invalid status code value: %d", value));
    }

    public int getVersion() {
        return version;
    }
}
