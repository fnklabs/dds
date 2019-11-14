package com.fnklabs.dds.storage;

import com.fnklabs.dds.DdsVersion;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

class MetaInformation {
    /**
     * | DDS_VERSION (4 bytes) | UUID (8 bytes) | Key length (4 bytes) | First block position (Long) | Last block position (long)
     */
    static final int DEFAULT_SIZE = Integer.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES;

    private final DdsVersion ddsVersion;
    private final UUID version;
    private final int keyLength;
    private final long positionOfFirstDataBlock;
    private final long positionOfFreeDataBlock;

    public MetaInformation(DdsVersion ddsVersion, UUID version, int keyLength, long positionOfFirstDataBlock, long positionOfFreeDataBlock) {
        this.ddsVersion = ddsVersion;
        this.version = version;
        this.keyLength = keyLength;
        this.positionOfFirstDataBlock = positionOfFirstDataBlock;
        this.positionOfFreeDataBlock = positionOfFreeDataBlock;
    }

    public DdsVersion getDdsVersion() {
        return ddsVersion;
    }

    public UUID getVersion() {
        return version;
    }

    public int getKeyLength() {
        return keyLength;
    }

    public long getPositionOfFirstDataBlock() {
        return positionOfFirstDataBlock;
    }

    public long getPositionOfFreeDataBlock() {
        return positionOfFreeDataBlock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetaInformation)) return false;
        MetaInformation that = (MetaInformation) o;
        return keyLength == that.keyLength &&
                positionOfFirstDataBlock == that.positionOfFirstDataBlock &&
                positionOfFreeDataBlock == that.positionOfFreeDataBlock &&
                ddsVersion == that.ddsVersion &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ddsVersion, version, keyLength, positionOfFirstDataBlock, positionOfFreeDataBlock);
    }

    static MetaInformation unpack(ByteBuffer buffer) {
        return new MetaInformation(
                DdsVersion.valueOf(buffer.getInt()),
                new UUID(buffer.getLong(), buffer.getLong()),
                buffer.getInt(),
                buffer.getLong(),
                buffer.getLong()
        );
    }

    static ByteBuffer pack(MetaInformation metaInformation) {
        ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_SIZE);

        buffer.putInt(metaInformation.ddsVersion.version);
        buffer.putLong(metaInformation.version.getMostSignificantBits());
        buffer.putLong(metaInformation.version.getLeastSignificantBits());
        buffer.putInt(metaInformation.keyLength);
        buffer.putLong(metaInformation.positionOfFirstDataBlock);
        buffer.putLong(metaInformation.positionOfFreeDataBlock);

        return buffer;
    }
}
