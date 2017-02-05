package com.fnklabs.dds.network;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TimeoutException extends RequestException {
    private final Message message;
}
