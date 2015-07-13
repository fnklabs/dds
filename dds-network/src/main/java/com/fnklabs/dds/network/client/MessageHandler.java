package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.connector.MessageBuffer;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Handler for processing system messages from server (notifications) or messages that wasn't awaiting
 */
class MessageHandler implements Runnable {
    private ExecutorService executorService;
    private BlockingQueue<MessageBuffer> inputMessages;
    private Consumer<MessageBuffer> messageBufferConsumer;

    public MessageHandler(ExecutorService executorService, BlockingQueue<MessageBuffer> inputMessages, Consumer<MessageBuffer> messageBufferConsumer) {
        this.executorService = executorService;
        this.inputMessages = inputMessages;
        this.messageBufferConsumer = messageBufferConsumer;
    }

    @Override
    public void run() {
        try {
            MessageBuffer message = inputMessages.poll(1, TimeUnit.MILLISECONDS);

            if (message != null) {
                messageBufferConsumer.accept(message);
            }
        } catch (InterruptedException e) {
            LoggerFactory.getLogger(getClass()).debug("Queue is empty");
        }

        executorService.submit(this);
    }
}
