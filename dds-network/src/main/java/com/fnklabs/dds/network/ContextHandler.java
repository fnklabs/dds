package com.fnklabs.dds.network;

import java.util.HashMap;
import java.util.Map;

/**
 * Message context handler.
 * <p>
 * Contains map of operation code to handler that can handle message by specified code
 */
public class ContextHandler {
    /**
     * Request handlers
     */
    private final Map<Integer, MessageHandler> handlers = new HashMap<>();

    public void addHandler(int operationCode, MessageHandler messageHandler) {
        handlers.put(operationCode, messageHandler);
    }

    protected MessageHandler getHandler(int operationCode) {
        return handlers.get(operationCode);
    }
}
