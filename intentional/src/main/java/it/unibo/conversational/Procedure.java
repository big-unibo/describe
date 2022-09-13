package it.unibo.conversational;

public interface Procedure<T> {
    void apply(final T p) throws Throwable;
}
