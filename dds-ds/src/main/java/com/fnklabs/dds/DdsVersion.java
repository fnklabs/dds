package com.fnklabs.dds;

public enum DdsVersion {
    VERSION_1(1),;

    public final int version;

    DdsVersion(int version) {
        this.version = version;
    }

    public static DdsVersion CURRENT = VERSION_1;

    public static DdsVersion valueOf(int value) {
        for (DdsVersion version : values()) {
            if (version.version == value) {
                return version;
            }
        }

        throw new IllegalArgumentException(String.format("Invalid version value: %d", value));
    }
}
