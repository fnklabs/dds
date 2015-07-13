package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.BytesUtils;

public class Token implements Comparable<Token> {


    private byte[] tokenValue;


    public Token(byte[] token) {
        tokenValue = token;
    }

    public byte[] getTokenValue() {
        return tokenValue;
    }


    @Override
    public int compareTo(Token o) {
        return BytesUtils.compare(tokenValue, o.getTokenValue());
    }
}
