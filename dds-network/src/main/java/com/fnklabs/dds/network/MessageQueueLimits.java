package com.fnklabs.dds.network;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class MessageQueueLimits extends Exception {
    private final Message message;

    Message message() {
        return message;
    }
}
