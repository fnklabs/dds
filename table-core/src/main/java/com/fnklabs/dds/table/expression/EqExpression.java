package com.fnklabs.dds.table.expression;

import java.util.Arrays;

public class EqExpression implements ExpressionEvaluator {
    @Override
    public boolean evaluate(byte[] a, byte[] b) {
        return Arrays.equals(a, b);
    }
}
