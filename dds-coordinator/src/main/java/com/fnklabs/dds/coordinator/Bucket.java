package com.fnklabs.dds.coordinator;

import com.google.common.base.Objects;

import java.util.concurrent.atomic.AtomicReference;

public class Bucket implements Comparable<Bucket> {
    private Token start;
    private Token end;

    private AtomicReference<State> state = new AtomicReference<>();

    public Bucket(Token start, Token end, State state) {
        this.start = start;
        this.end = end;
        this.state.set(state);
    }

    public State getState() {
        return state.get();
    }

    public Token getStart() {
        return start;
    }

    public Token getEnd() {
        return end;
    }

    public boolean changeState(State expectingState, State newState) {
        return state.compareAndSet(expectingState, newState);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Bucket) {
            return Objects.equal(((Bucket) obj).getStart(), getStart()) && Objects.equal(((Bucket) obj).getEnd(), getEnd());
        }

        return false;
    }

    public boolean contains(Token key) {
        return getStart().compareTo(key) <= 0 && getEnd().compareTo(key) >= 0;
    }

    @Override
    public int compareTo(Bucket o) {
        return getStart().compareTo(o.getStart());
    }

    public enum State {
        OK, BALANCING, REMOVE
    }
}
