package com.fnklabs.dds.table.expression;

import com.fnklabs.dds.table.Expression;

import java.util.HashMap;
import java.util.Map;

public final class EvaluatorFactory {
    private final static Map<Expression, ExpressionEvaluator> EVALUATORS = new HashMap<>();

    static {
        EVALUATORS.put(Expression.NEQ, new NeqExpression());
        EVALUATORS.put(Expression.EQ, new EqExpression());
    }

    private EvaluatorFactory() {
    }

    public static ExpressionEvaluator get(Expression expression) {
        return EVALUATORS.get(expression);
    }

}
