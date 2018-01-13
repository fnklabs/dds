package com.fnklabs.dds.table.query;

import java.util.function.Predicate;

public class Condition<T> implements Predicate<T> {
    private final Predicate<T> condition;

    public Condition(Predicate<T> condition) {
        this.condition = condition;
    }

    @Override
    public boolean test(T t) {
        return condition.test(t);
    }
}
