package com.fnklabs.dds.network;

public enum ApiVersion {
    VERSION_1(1);

    static final ApiVersion CURRENT = ApiVersion.VERSION_1;

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
