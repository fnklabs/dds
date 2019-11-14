package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.Message;

class MessageQueueLimits extends Exception {
    private final Message message;

    MessageQueueLimits(Message message) {this.message = message;}

    Message message() {
        return message;
    }
}
