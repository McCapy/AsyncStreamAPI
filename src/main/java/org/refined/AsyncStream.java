package org.refined;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;

@SuppressWarnings({"unchecked","unused"})
public final class AsyncStream<T> extends AsynchronousStream<T> {

    public AsyncStream() {
        super();
    }

    public AsyncStream(T... values) {
        super(values);
    }

    private AsyncStream(@NotNull StreamScope scope) {
        super(scope);
    }

    public AsyncStream(Collection<T> collection) {
        super(collection);
    }

    public static <R> AsynchronousStream<R> of(Collection<R> collection) {
        return new AsyncStream<>(collection);
    }

    public static AsynchronousStream<Void> ofEmpty() {
        return new AsyncStream<>();
    }

    public static <R> AsynchronousStream<R> of(R... items) {
        return new AsyncStream<>(items);
    }
    @Override
    <R> AsynchronousStream<R> repack(StreamScope scope) {
        return new AsyncStream<>(scope);
    }
}