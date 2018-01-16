package com.fnklabs.dds.table.expression;

import com.fnklabs.dds.BytesUtils;

public class NeqExpression implements ExpressionEvaluator {
    @Override
    public boolean evaluate(byte[] a, byte[] b) {
        return BytesUtils.compare(a, b) != 0;
    }
}
