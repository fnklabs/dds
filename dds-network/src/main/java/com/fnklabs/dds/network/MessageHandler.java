package com.fnklabs.dds.network;


import com.fnklabs.dds.network.connector.Message;
import com.fnklabs.dds.network.exception.RequestException;
import org.springframework.stereotype.Service;

/**
 * Message request handler
 *
 * @param <I> Input message data type
 * @param <O> Output message data type
 */
@Service
public interface MessageHandler<I, O> {

    /**
     * Handle request
     *
     * @param requestBuffer Request
     *
     * @return ByteBuffer response
     *
     * @throws RequestException
     */
    Message<O> handle(Message<I> requestBuffer) throws RequestException;
}
