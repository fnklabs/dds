package com.fnklabs.dds.coordinator.operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

public class Ping implements Operation {
    /**
     * DateTime when ping request was send/created
     */
    private final DateTime sendTime = DateTime.now();

    /**
     * DateTime when ping request was processed by remote node
     */
    private DateTime processedTime;

    /**
     * DateTime when ping reply was received
     */
    private DateTime receivedTime;

    public Ping() {
    }

    public Ping(@NotNull DateTime processedTime) {
        this.processedTime = processedTime;
    }

    public Ping(@NotNull DateTime processedTime, @NotNull DateTime receivedTime) {
        this.processedTime = processedTime;
        this.receivedTime = receivedTime;
    }

    public DateTime getSendTime() {
        return sendTime;
    }

    @Nullable
    public DateTime getProcessedTime() {
        return processedTime;
    }

    @Nullable
    public DateTime getReceivedTime() {
        return receivedTime;
    }
}
