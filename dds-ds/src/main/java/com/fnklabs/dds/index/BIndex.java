package com.fnklabs.dds.storage;

import com.fnklabs.dds.BytesUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class BIndex implements Index {
    private static final double MAX_DEPTH_DIFF = 0.3;
    private static final Logger LOGGER = LoggerFactory.getLogger(BIndex.class);
    private static final int HEADER_START_POSITION = 0;
    private static final int HEADER_VERSION_POSITION = 0;
    private static final int HEADER_VERSION_LENGTH = Integer.BYTES;
    private static final int HEADER_KEY_SIZE_POSITION = HEADER_VERSION_POSITION + HEADER_VERSION_LENGTH;
    private static final int HEADER_KEY_SIZE_LENGTH = Integer.BYTES;
    private static final int HEADER_LENGTH = HEADER_VERSION_LENGTH + HEADER_KEY_SIZE_LENGTH;
    private static final long DATA_ROOT_NODE_START_POSITION = HEADER_LENGTH;
    private static final int VERSION = 1;
    private final FileChannel indexFileChannel;

    private final AtomicLong freePosition = new AtomicLong(DATA_ROOT_NODE_START_POSITION);

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Length of index key
     */
    private final int keySize;

    private final ThreadLocal<ByteBuffer> buffer;


    private BIndex(FileChannel fileChannel, int keySize) {
        this.indexFileChannel = fileChannel;
        this.keySize = keySize;
        buffer = ThreadLocal.withInitial(() -> NodeUtils.createByteBuffer(keySize));
    }

    public static BIndex create(File file, int keySize) throws IndexIOException, IndexExists {

        if (file.exists()) {
            throw new IndexExists();
        }

        try {
            FileChannel fileChannel = createFileChannel(file);

            BIndex index = new BIndex(fileChannel, keySize);

            index.writeHeader(new Header(VERSION, keySize));

            Node rootNode = Node.builder()
                                .withPosition((long) DATA_ROOT_NODE_START_POSITION)
                                .withKey(new byte[keySize])
                                .build();

            index.writeNode(rootNode);


            return index;
        } catch (IOException e) {
            throw new IndexIOException(e);
        }
    }

    public static BIndex open(File file) throws IndexNotExists, IndexIOException, IndexIsBroken {
        if (file.exists()) {
            throw new IndexNotExists();
        }

        try {
            FileChannel fileChannel = createFileChannel(file);

            Optional<Header> header = readHeader(fileChannel);

            if (!header.isPresent()) {
                throw new IndexIsBroken();
            }

            return new BIndex(fileChannel, header.get().getKeySize());
        } catch (IOException e) {
            throw new IndexIOException(e);
        }

    }

    /**
     * {@inheritDoc}
     *
     * @param key
     */
    @Override
    public Optional<Long> get(byte[] key) {
        Node parentNode = getRootNode();


        for (; ; ) {
            if (parentNode.hasChild()) {
                Optional<Node> leftNode = parentNode.getLeftNodeReference().flatMap(this::readNode);
                Optional<Node> rightNode = parentNode.getRightNodeReference().flatMap(this::readNode);

                if (leftNode.isPresent()) {
                    Node node = leftNode.get();

                    if (BytesUtils.compare(key, node.getKey()) == 0) {
                        return node.getDataReference();
                    } else if (BytesUtils.compare(key, node.getKey()) < 0) {
                        parentNode = leftNode.get();
                        continue;
                    }
                }

                if (rightNode.isPresent()) {
                    Node node = rightNode.get();

                    if (BytesUtils.compare(key, node.getKey()) == 0) {
                        return node.getDataReference();
                    } else if (BytesUtils.compare(key, node.getKey()) > 0) {
                        parentNode = rightNode.get();
                    }
                }
            } else {
                break;
            }

        }

        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     *
     * @param key
     * @param dataPosition
     */
    @Override
    public boolean put(byte[] key, long dataPosition) {
        Lock lock = readWriteLock.writeLock();

        LOGGER.debug("Put key {}", new BigInteger(key));

        try {
            lock.lock();

            Node newNode = Node.builder()
                               .withKey(key)
                               .withDataReference(dataPosition)
                               .build();

            Node rootNode = getRootNode();
            Node parentNode = rootNode;

            List<Node> touchedNode = new ArrayList<>();

            for (; ; ) {
                touchedNode.add(parentNode);

                Node node = tryAddToNode(parentNode, newNode, newNodeParent -> onDepthChanged(touchedNode, rootNode));

                if (Objects.equals(parentNode, node)) {
                    LOGGER.debug("Node are equals, skip add operation {} {}", node, parentNode);
                    break;
                } else {
                    parentNode = node;
                }

            }
            rebalanceTree(rootNode);

        } finally {
            lock.unlock();
        }

        return true;
    }

    private void rebalanceTree(final Node rootNode) {
        int attempts = 0;

//        for (; ; ) {
        // check whether tree is not well balanced

        Optional<Node> leftNodeOpt = getRootNode().getLeftNodeReference().flatMap(this::readNode);
        Optional<Node> rightNodeOpt = getRootNode().getRightNodeReference().flatMap(this::readNode);

        long leftNodeDepth = leftNodeOpt.map(Node::getDepth).orElse(0L);
        long rightNodeDepth = rightNodeOpt.map(Node::getDepth).orElse(0L);

        LOGGER.debug("Root node balance: {}/{}", leftNodeDepth, rightNodeDepth);

        long maxDepth = Long.max(leftNodeDepth, rightNodeDepth);

        float depthDiff = (float) Math.abs(leftNodeDepth - rightNodeDepth) / maxDepth;

        if (depthDiff < MAX_DEPTH_DIFF || attempts > 1) {
            return;
        }
        // balance tree
        if (leftNodeDepth < rightNodeDepth) {
            Node rightNode = rightNodeOpt.orElseThrow(() -> new RuntimeException("Something gone wrong"));


            Node newLeftNode = Node.builder()
                                   .from(rightNode)
                                   .withRightNodeReference(rightNode.getLeftNodeReference().orElse(null))
                                   .withLeftNodeReference(rootNode.getLeftNodeReference().orElse(null))
                                   .build();

            writeNode(updateNodeDepth(newLeftNode));

            Node newRootNode = Node.builder()
                                   .from(rootNode)
                                   .withLeftNodeReference(rightNode.getPosition().orElse(null))
                                   .withRightNodeReference(rightNode.getRightNodeReference().orElse(null))
                                   .build();

            writeNode(updateNodeDepth(newRootNode));

        } else {

        }

        attempts++;
//        }
    }

    private Node updateNodeDepth(Node node) {
        Optional<Node> leftNode = node.getLeftNodeReference().flatMap(this::readNode);
        Optional<Node> rightNode = node.getRightNodeReference().flatMap(this::readNode);

        long maxDepth = Long.max(leftNode.map(Node::getDepth).orElse(0L), rightNode.map(Node::getDepth).orElse(0L));

        Node newNode = Node.builder()
                           .from(node)
                           .withDepth(maxDepth)
                           .build();

        return newNode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long length() throws IndexIOException {
        return (size() - HEADER_LENGTH) / NodeUtils.getBufferLength(keySize) - 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size() throws IndexIOException {
        try {
            return indexFileChannel.size();
        } catch (IOException e) {
            LOGGER.warn("Can't get index size");

            throw new IndexIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        indexFileChannel.close();
    }

    private void onDepthChanged(List<Node> touchedNodes, Node rootNode) {
        if (rootNode.getDepth() < touchedNodes.size()) {

            List<Node> nodes = touchedNodes.stream()
                                           .sorted(Comparator.comparing(Node::getDepth).reversed())
                                           .collect(Collectors.toList());

            int currentDepth = 0;
            for (Node node : nodes) {
                currentDepth++;
                if (node.getDepth() < currentDepth) {
                    Node updateNode = Node.builder()
                                          .from(node)
                                          .withDepth(currentDepth)
                                          .build();

                    writeNode(updateNode);
                }

            }


        }
    }

    /**
     * Add newNode to specified parent node.
     * <p>
     * If Return Node == Parent.Node than new Node was added to provided parent node else was returned candidate for add operation it's realized so to avoid
     * {@link StackOverflowError}
     *
     * @param parentNode Parent node to which we must add newNode
     * @param newNode    New node
     *
     * @return Node to which was added or can be added newNode.
     */
    @Nullable
    private Node tryAddToNode(Node parentNode, Node newNode, Consumer<Node> onDepthChange) {
        LOGGER.debug("Try add node {} to {}", newNode, parentNode);

        if (parentNode.getPosition().orElse(0L) != DATA_ROOT_NODE_START_POSITION && Objects.equals(parentNode, newNode)) { // replace node
            LOGGER.debug("Node are equals {} {} replace it", parentNode, newNode);

            Node mergedNode = NodeUtils.merge(parentNode, newNode);

            writeNode(mergedNode);

            return parentNode;
        }

        if (parentNode.hasLessThanTwoChild()) {
            if (!parentNode.getLeftNodeReference().isPresent()) {
                updateLeftNode(parentNode, newNode);

                onDepthChange.accept(newNode);
            } else if (!parentNode.getRightNodeReference().isPresent()) {
                writeRightNode(parentNode, newNode);
            }

            return parentNode;
        }

        Node leftNode = parentNode.getLeftNodeReference().flatMap(this::readNode).orElse(null);
        Node rightNode = parentNode.getRightNodeReference().flatMap(this::readNode).orElse(null);


        if (newNode.compareTo(leftNode) <= 0) { // node keys are equals than replace record
            return leftNode;
        } else if (newNode.compareTo(rightNode) >= 0) {
            return rightNode;
        }

        LOGGER.debug("Something gone wrong");

        return null;
    }

    private void updateLeftNode(Node parentNode, Node leftNode) {
        long leftNodePosition = appendNode(leftNode);

        Node replaceableNode = Node.builder()
                                   .withKey(parentNode.getKey())
                                   .withPosition(parentNode.getPosition().orElse(null))
                                   .withDataReference(parentNode.getDataReference().orElse(null))
                                   .withDepth(parentNode.getDepth())
                                   .withLeftNodeReference(leftNodePosition)
                                   .withRightNodeReference(parentNode.getRightNodeReference().orElse(null))
                                   .build();


        writeNode(replaceableNode);
    }

    private void writeRightNode(Node parentNode, Node leftNode) {
        long rightNodePosition = appendNode(leftNode);

        Node replaceableNode = Node.builder()
                                   .withKey(parentNode.getKey())
                                   .withPosition(parentNode.getPosition().orElse(null))
                                   .withDataReference(parentNode.getDataReference().orElse(null))
                                   .withDepth(parentNode.getDepth())
                                   .withLeftNodeReference(parentNode.getLeftNodeReference().orElse(null))
                                   .withRightNodeReference(rightNodePosition)
                                   .build();

        writeNode(replaceableNode);
    }

    private Node getRootNode() {
        return readNode(DATA_ROOT_NODE_START_POSITION).orElseThrow(() -> new RuntimeException());
    }

    private Optional<Node> readNode(long position) {
        ByteBuffer buffer = this.buffer.get();

        try {

            LOGGER.debug("Reading node by position: {}", position);

            read(buffer, position);

            Optional<Node> node = Optional.of(NodeUtils.fromByteBuffer(buffer));

            node.ifPresent(val -> LOGGER.debug("Read node: {}", node));

            return node;
        } catch (IndexIOException e) {
            LOGGER.warn("Can't process index", e);
        } finally {
            buffer.clear();
        }

        return Optional.empty();
    }

    private boolean writeNode(Node node) {
        ByteBuffer byteBuffer = buffer.get();

        try {

            NodeUtils.writeToBuffer(node, byteBuffer);

            write(byteBuffer, node.getPosition().orElseThrow(() -> new RuntimeException()));

            LOGGER.debug("Write node: {}", node);

            return true;
        } catch (IndexIOException e) {
            LOGGER.warn("Can't process index", e);
        } finally {
            byteBuffer.clear();
        }

        return false;
    }

    private long appendNode(Node node) {
        long position = freePosition.addAndGet(NodeUtils.getBufferLength(keySize));

        Node persistentValue = Node.builder()
                                   .withKey(node.getKey())
                                   .withPosition(position)
                                   .withDataReference(node.getDataReference().orElse(null))
                                   .withDepth(node.getDepth())
                                   .withLeftNodeReference(node.getLeftNodeReference().orElse(null))
                                   .withRightNodeReference(node.getRightNodeReference().orElse(null))
                                   .build();

        writeNode(persistentValue);

        return position;
    }

    private void writeHeader(Header header) throws IndexIOException {
        write(Header.toByteBuffer(header), 0);
    }

    private int read(ByteBuffer buffer, long position) throws IndexIOException {
        try {
            return IOUtils.read(indexFileChannel, buffer, position);
        } catch (IOException e) {
            throw new IndexIOException(e);
        }
    }

    private int write(ByteBuffer buffer, long position) throws IndexIOException {
        try {
            return IOUtils.write(indexFileChannel, buffer, position);
        } catch (IOException e) {
            throw new IndexIOException(e);
        }
    }

    private static Optional<Header> readHeader(FileChannel fileChannel) throws IndexIOException {
        ByteBuffer buffer = Header.createBuffer();

        try {
            int read = IOUtils.read(fileChannel, buffer, HEADER_START_POSITION);

            if (read < HEADER_LENGTH) {
                return Optional.empty();
            }

            return Optional.of(Header.fromByteBuffer(buffer));
        } catch (IOException e) {
            LOGGER.error("Can't read header", e);

            return Optional.empty();
        }
    }

    private static FileChannel createFileChannel(File file) throws IOException {
        return FileChannel.open(
                file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.DSYNC
        );
    }

    private static class Header {
        private final int keySize;
        private final int version;

        private Header(int version, int keySize) {
            this.keySize = keySize;
            this.version = version;
        }

        static Header fromByteBuffer(ByteBuffer buffer) {
            return new Header(buffer.getInt(HEADER_VERSION_POSITION), buffer.getInt(HEADER_KEY_SIZE_POSITION));
        }

        static ByteBuffer toByteBuffer(Header header) {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_LENGTH);
            buffer.putInt(HEADER_VERSION_POSITION, header.getVersion());
            buffer.putInt(HEADER_KEY_SIZE_POSITION, header.getKeySize());

            return buffer;
        }

        static ByteBuffer createBuffer() {
            return ByteBuffer.allocate(HEADER_LENGTH);
        }

        int getVersion() {
            return version;
        }

        int getKeySize() {
            return keySize;
        }
    }
}
