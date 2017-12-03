package com.fnklabs.dds.network.pool;

import com.fnklabs.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


@Slf4j
public class NioExecutor implements NetworkExecutor {

    private final ExecutorService opAcceptExecutor;
    private final ExecutorService opReadExecutor;
    private final ExecutorService opWriteExecutor;

    private final Selector opAcceptSelector;
    private final Selector opWriteSelector;
    private final Selector opReadSelector;

    private final int closeTimeout;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final Map<Channel, Consumer<SelectionKey>> opAcceptConsumers = new ConcurrentHashMap<>();
    private final Map<Channel, Consumer<SelectionKey>> opReadConsumers = new ConcurrentHashMap<>();
    private final Map<Channel, Consumer<SelectionKey>> opWriteConsumers = new ConcurrentHashMap<>();

    private NioExecutor(ExecutorService opAcceptExecutor,
                        ExecutorService opReadExecutor,
                        ExecutorService opWriteExecutor,
                        int closeTimeout) throws IOException {
        this.opAcceptExecutor = opAcceptExecutor;
        this.opReadExecutor = opReadExecutor;
        this.opWriteExecutor = opWriteExecutor;

        this.closeTimeout = closeTimeout;

        this.opAcceptSelector = Selector.open();
        this.opWriteSelector = Selector.open();
        this.opReadSelector = Selector.open();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void run() {
        if (isRunning.compareAndSet(false, true)) {
            opAcceptExecutor.submit(() -> {
                while (isRunning.get()) {
                    processKeys(opAcceptSelector, key -> {
                        opAcceptConsumers.get(key.channel()).accept(key);
                    });
                }
            });

            opReadExecutor.submit(() -> {
                while (isRunning.get()) {
                    processKeys(opReadSelector, key -> {
                        opReadConsumers.get(key.channel()).accept(key);
                    });
                }
            });


            opWriteExecutor.submit(() -> {
                while (isRunning.get()) {
                    processKeys(opWriteSelector, key -> {
                        opWriteConsumers.get(key.channel()).accept(key);
                    });
                }
            });
        }
    }

    @Override
    public void shutdown() {
        isRunning.compareAndSet(true, false);

        try {
            opAcceptExecutor.shutdown();
            opReadExecutor.shutdown();
            opWriteExecutor.shutdown();

            try {
                opAcceptExecutor.awaitTermination(closeTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

            try {
                opReadExecutor.awaitTermination(closeTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

            try {
                opWriteExecutor.awaitTermination(closeTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // todo add normal exception
        }
    }

    @Override
    public SelectionKey registerOpAccept(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException {
        opAcceptConsumers.putIfAbsent(channel, consumer);
        return channel.register(opAcceptSelector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public SelectionKey registerOpRead(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException {
        opReadConsumers.putIfAbsent(channel, consumer);

        opReadSelector.wakeup();

        return channel.register(opReadSelector, SelectionKey.OP_READ);
    }

    @Override
    public SelectionKey registerOpWrite(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException {
        opWriteConsumers.putIfAbsent(channel, consumer);

        opReadSelector.wakeup();

        return channel.register(opWriteSelector, SelectionKey.OP_WRITE);
    }

    private static void processKeys(Selector selector, Consumer<SelectionKey> consumer) {
        try {
            int select = selector.select(10);

            if (select > 0) {
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> keysIterator = keys.iterator();

                log.debug("new selector keys: {}/{}", select, keys.size());

                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();

                    log.debug("new key: {}", key.attachment());

                    if (!key.isValid()) {
                        log.debug("key `{}` is invalid", key.attachment());
                        continue;
                    }

                    consumer.accept(key);

                    keysIterator.remove();
                }
            }
        } catch (Exception e) {
            log.warn("cant process selector {}", selector, e);
        }
    }


    public static class Builder {
        private int opAcceptExecutor = 1;
        private int opReadExecutor = 1;
        private int opWriteExecutor = 1;
        private int closeTimeout = 60_000; // 60 sec

        public NetworkExecutor build() throws IOException {
            return new NioExecutor(
                    Executors.fixedPoolExecutor(opAcceptExecutor, "dds.nio.accept"),
                    Executors.fixedPoolExecutor(opReadExecutor, "dds.nio.read"),
                    Executors.fixedPoolExecutor(opWriteExecutor, "dds.nio.write"),
                    closeTimeout
            );
        }

        public Builder setOpAcceptExecutor(int opAcceptExecutor) {
            this.opAcceptExecutor = opAcceptExecutor;

            return this;
        }

        public Builder setOpReadExecutor(int opReadExecutor) {
            this.opReadExecutor = opReadExecutor;

            return this;
        }

        public Builder setOpWriteExecutor(int opWriteExecutor) {
            this.opWriteExecutor = opWriteExecutor;

            return this;
        }

        public Builder setCloseTimeout(int closeTimeout) {
            this.closeTimeout = closeTimeout;

            return this;
        }
    }
}
