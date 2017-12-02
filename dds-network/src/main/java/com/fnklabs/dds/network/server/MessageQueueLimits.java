package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.Message;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class MessageQueueLimits extends Exception {
    private final Message message;

    Message message() {
        return message;
    }
}
