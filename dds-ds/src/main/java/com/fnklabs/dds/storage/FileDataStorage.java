package com.fnklabs.dds.storage;

import com.fnklabs.dds.IOUtils;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public class FileDataStorage implements DataStorage {
    static final long DATA_START_OFFSET = MetaInformation.DEFAULT_SIZE;

    private final FileChannel fileChannel;
    private final AtomicReference<MetaInformation> metaInformation = new AtomicReference<>();

    private final AtomicLong firstDataBlockPosition = new AtomicLong();
    private final AtomicLong lastDataBlockFreePosition = new AtomicLong();

    private FileDataStorage(FileChannel fileChannel, MetaInformation metaInformation) {
        this.fileChannel = fileChannel;
        this.metaInformation.compareAndSet(null, metaInformation);

        firstDataBlockPosition.set(metaInformation.getPositionOfFirstDataBlock());
        lastDataBlockFreePosition.set(metaInformation.getPositionOfFreeDataBlock());
    }

    public static FileDataStorage open(Path path, MetaInformation defaultMetaInformation) throws IOException {
        FileChannel channel = FileChannel.open(
                path,
                Sets.newHashSet(
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ
                ),
                PosixFilePermissions.asFileAttribute(Sets.newHashSet(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ))
        );

        MetaInformation metaInformation = readMetaInformation(channel);

        if (metaInformation == null) {
            metaInformation = new MetaInformation(
                    defaultMetaInformation.getDdsVersion(),
                    defaultMetaInformation.getVersion(),
                    defaultMetaInformation.getKeyLength(),
                    defaultMetaInformation.getPositionOfFirstDataBlock(),
                    DATA_START_OFFSET
            );


            writeMetaInformation(channel, metaInformation);
        }

        return new FileDataStorage(channel, metaInformation);
    }

    @Override
    public DataBlock get(byte[] dataKey) {
        DataBlockEntry dataBlock = getDataBlock(dataKey);

        return dataBlock != null ? dataBlock.dataBlock : null;
    }

    @Nullable
    @Override
    public DataBlock get(long position) throws IOException {
        return readDataBlock(metaInformation.get(), fileChannel, position);
    }

    @Override
    public long put(byte[] dataKey, byte[] data) throws IOException {
        try (Timer timer = MetricsFactory.getMetrics().getTimer("dds.io.put")) {

            AtomicReference<DataBlockEntry> prevValueRef = new AtomicReference<>();
            AtomicReference<DataBlockEntry> dataBlockRef = new AtomicReference<>();

            synchronized (dataKey) {
                scan((position, dataBlock) -> {
                    boolean equals = Arrays.equals(dataKey, dataBlock.getKey());

                    if (!equals) {
                        prevValueRef.set(new DataBlockEntry(position, dataBlock));
                    }

                    return equals;
                }, (position, dataBlock) -> {
                    dataBlockRef.set(new DataBlockEntry(position, dataBlock));

                    return false;
                });

                int dataSize = DataBlock.length(dataKey, data);

                DataBlockEntry dataBlockEntry = dataBlockRef.get();

                if (dataBlockEntry == null) {
                    return addNewBlock(dataKey, data);
                } else {
                    DataBlock dataBlock = dataBlockEntry.dataBlock;

                    if (dataBlock.length() <= dataSize) {
                        ByteBuffer buffer = DataBlock.pack(new DataBlock(dataKey, data, dataBlock.getHeader().getNextBlockPosition(), 1));
                        buffer.flip();

                        IOUtils.write(fileChannel, buffer, dataBlockEntry.position);

                        return dataBlockEntry.position;
                    } else {
                        long newBlockPosition = addNewBlock(dataKey, data);

                        DataBlockEntry prevBlockEntry = prevValueRef.get();

                        if (prevBlockEntry != null) {
                            DataBlock prevDataBlock = prevBlockEntry.dataBlock;
                            ByteBuffer buffer = DataBlock.pack(
                                    new DataBlock(
                                            prevDataBlock.getKey(), prevDataBlock.getData(), newBlockPosition, 1
                                    )
                            );
                            buffer.flip();

                            IOUtils.write(fileChannel, buffer, prevBlockEntry.position);
                        }

                        return newBlockPosition;
                    }
                }
            }
        }
    }

    private long addNewBlock(byte[] dataKey, byte[] data) throws IOException {
        for (; ; ) {
            Long lastPosition = lastDataBlockFreePosition.get();

            long nextDataPosition = lastPosition + DataBlock.length(dataKey, data);

            if (lastDataBlockFreePosition.compareAndSet(lastPosition, nextDataPosition)) {

                ByteBuffer buffer = DataBlock.pack(new DataBlock(dataKey, data, nextDataPosition, 1));
                buffer.flip();

                IOUtils.write(fileChannel, buffer, lastPosition);

                if (firstDataBlockPosition.get() == 0) { // on adding first block
                    firstDataBlockPosition.compareAndSet(0, lastPosition);
                }

                return lastPosition;
            }
        }
    }

    @Override
    public long scan(BiPredicate<Long, DataBlock> predicate, BiFunction<Long, DataBlock, Boolean> consumer) {
        AtomicLong entries = new AtomicLong();

        try (Timer timer = MetricsFactory.getMetrics().getTimer("dds.io.scan")) {

            long dataBlockPosition = firstDataBlockPosition.get();

            if (dataBlockPosition == 0) {
                return entries.get();
            }

            for (; ; ) {
                DataBlock dataBlock = readDataBlock(metaInformation.get(), fileChannel, dataBlockPosition);

                if (dataBlock == null) {
                    break;
                }

                boolean test = predicate.test(dataBlockPosition, dataBlock);

                if (test) {
                    entries.incrementAndGet();
                }

                if (test && !consumer.apply(dataBlockPosition, dataBlock)) {
                    break;
                }

                long nextBlockPosition = dataBlock.getHeader().getNextBlockPosition();

                if (nextBlockPosition == 0) {
                    break;
                }

                dataBlockPosition = nextBlockPosition;
            }

            return entries.get();
        } catch (Exception e) {
            LoggerFactory.getLogger(FileDataStorage.class).warn("Can't scan ", e);
        }

        return entries.get();
    }

    @Nullable
    private DataBlockEntry getDataBlock(byte[] dataKey) {
        if (firstDataBlockPosition.get() <= 0) {
            return null;
        }

        AtomicReference<DataBlockEntry> dataBlockRef = new AtomicReference<>();

        scan(
                (position, dataBlock) -> Arrays.equals(dataKey, dataBlock.getKey()),
                (position, dataBlock) -> {
                    dataBlockRef.set(new DataBlockEntry(position, dataBlock));

                    return false;
                }
        );

        return dataBlockRef.get();
    }

    private static DataBlock readDataBlock(MetaInformation metaInformation, FileChannel channel, long position) throws IOException {
        try (Timer timer = MetricsFactory.getMetrics().getTimer("dds.io.read-block")) {
            ByteBuffer buffer = IOUtils.allocate(DataBlock.Header.HEADER_SIZE);

            if (IOUtils.read(channel, buffer, position) == DataBlock.Header.HEADER_SIZE) {
                buffer.flip();

                DataBlock.Header header = DataBlock.Header.unpack(buffer);

                int dataLength = header.getLength() - DataBlock.Header.HEADER_SIZE;

                ByteBuffer dataBuffer = IOUtils.allocate(dataLength);

                long dataPosition = position + DataBlock.Header.HEADER_SIZE;

                if (IOUtils.read(channel, dataBuffer, dataPosition) == dataLength) {
                    dataBuffer.flip();
                    return DataBlock.unpack(metaInformation, header, dataBuffer);
                }
            }
        }

        return null;
    }

    @Nullable
    private static MetaInformation readMetaInformation(FileChannel channel) throws IOException {
        ByteBuffer buffer = IOUtils.allocate(MetaInformation.DEFAULT_SIZE);

        if (IOUtils.read(channel, buffer, 0) == MetaInformation.DEFAULT_SIZE) {
            buffer.flip();

            return MetaInformation.unpack(buffer);
        }

        return null;
    }

    private static void writeMetaInformation(FileChannel channel, MetaInformation metaInformation) throws IOException {
        ByteBuffer byteBuffer = MetaInformation.pack(metaInformation);
        byteBuffer.rewind();

        IOUtils.write(channel, byteBuffer, 0);
    }

    @RequiredArgsConstructor
    private static class DataBlockEntry {
        private final long position;
        private final DataBlock dataBlock;

    }
}
