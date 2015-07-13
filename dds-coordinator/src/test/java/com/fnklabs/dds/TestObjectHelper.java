package com.fnklabs.dds;

import com.fnklabs.dds.coordinator.Record;

import java.math.BigDecimal;
import java.util.UUID;

public class TestObjectHelper implements Record {
    private UUID id = UUID.randomUUID();
    private UUID user = UUID.randomUUID();
    private String group;
    private String customer;
    private String paymentId;
    private BigDecimal revenue;

    public TestObjectHelper(UUID user, int group, int customer, int paymentId, BigDecimal revenue) {
        this.user = user;
        this.group = String.valueOf(group);
        this.customer = String.valueOf(customer);
        this.paymentId = String.valueOf(paymentId);
        this.revenue = revenue;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getUser() {
        return user;
    }

    public void setUser(UUID user) {
        this.user = user;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void setRevenue(BigDecimal revenue) {
        this.revenue = revenue;
    }

    @Override
    public byte[] getKey() {
        return UUID.randomUUID().toString().getBytes();
    }
}
