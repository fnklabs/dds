package com.fnklabs.dds.storage;

import com.fnklabs.dds.DdsVersion;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.util.UUID;

@RequiredArgsConstructor
@EqualsAndHashCode(of = {
        "ddsVersion", "version", "keyLength", "positionOfFirstDataBlock", "positionOfFreeDataBlock"
})
@ToString
@Getter
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
