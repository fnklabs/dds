package com.fnklabs.dds.storage;

import com.fnklabs.dds.IOUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;

/**
 * Storage data block
 * <pre>
 *
 * Block length | Version |Next block position | Key |Data
 Type
 int
 int
 long
 byte[]
 byte[]
 Offset
 0
 4
 8
 16
 16 + key.length
 * </pre>
 * <p>
 * <p>
 * <b>Storage Data block schema</b>
 * <p>
 * | MetaInformation | Data Block[0] |...| Data Block[n]
 */

@Getter
public class DataBlock {
    private final Header header;
    private final byte[] key;
    private final byte[] data;

//    public DataBlock(byte[] key, byte[] value) {
//
//    }

    DataBlock(byte[] key, byte[] data, long nextBlockPosition, int version) {
        int blockLength = Header.HEADER_SIZE + key.length + data.length;
        header = new Header(blockLength, version, nextBlockPosition);
        this.key = key;
        this.data = data;
    }


    private DataBlock(Header header, byte[] key, byte[] data) {
        this.header = header;
        this.key = key;
        this.data = data;
    }

    public static ByteBuffer pack(DataBlock block) {
        ByteBuffer buffer = Header.pack(block.header);

        buffer.put(block.key);
        buffer.put(block.data);

        return buffer;
    }

    public static DataBlock unpack(MetaInformation metaInformation, Header header, ByteBuffer buffer) {
        int dataLength = header.length - Header.HEADER_SIZE - metaInformation.getKeyLength();

        byte[] key = new byte[metaInformation.getKeyLength()];
        byte[] data = new byte[dataLength];

        buffer.get(key);
        buffer.get(data);

        return new DataBlock(header, key, data);
    }

    public int length() {
        return getHeader().getLength();
    }

    public static int length(byte[] dataKey, byte[] data) {
        return DataBlock.Header.HEADER_SIZE + dataKey.length + data.length;
    }

    @RequiredArgsConstructor
    @Getter
    static class Header {
        static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES;

        /** data block length */
        private final int length;
        /** data block version */
        private final int version;
        /** pointer on next block */
        private final long nextBlockPosition;

        public static ByteBuffer pack(Header header) {
            int length = header.length;

            ByteBuffer buffer = IOUtils.allocate(length);

            buffer.putInt(length);
            buffer.putInt(header.version);
            buffer.putLong(header.nextBlockPosition);

            return buffer;
        }

        public static Header unpack(ByteBuffer byteBuffer) {
            int length = byteBuffer.getInt();
            int version = byteBuffer.getInt();
            long nextBlockPosition = byteBuffer.getLong();


            return new Header(length, version, nextBlockPosition);
        }
    }
}
