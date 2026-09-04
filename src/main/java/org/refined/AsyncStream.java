package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.*;

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

    public static <R> AsyncStream<R> of(Collection<R> collection) {
        return new AsyncStream<>(collection);
    }

    public static AsyncStream<Void> ofEmpty() {
        return new AsyncStream<>();
    }

    public static <R> AsyncStream<R> of(R... items) {
        return new AsyncStream<>(items);
    }

    @Override
    public AsyncStream<T> start() {
        return (AsyncStream<T>) super.start();
    }

    @Override
    public void cancel() {
        super.cancel();
    }

    @Override
    public AsyncStream<T> filter(Predicate<T> predicate) {
        return (AsyncStream<T>) super.filter(predicate);
    }

    @Override
    public <R> AsyncStream<R> map(Function<T, R> function) {
        return (AsyncStream<R>) super.map(function);
    }

    @Override
    public <R> AsyncStream<R> offer(R... items) {
        return (AsyncStream<R>) super.offer(items);
    }

    @Override
    public <R> AsyncStream<R> offer(Collection<R> items) {
        return (AsyncStream<R>) super.offer(items);
    }

    @Override
    public AsyncStream<T> offer(Function<List<T>, List<T>> function) {
        return (AsyncStream<T>) super.offer(function);
    }

    @Override
    public AsyncStream<Void> empty(Runnable runnable) {
        return (AsyncStream<Void>) super.empty(runnable);
    }

    @Override
    public AsyncStream<Void> empty() {
        return (AsyncStream<Void>) super.empty();
    }

    @Override
    public AsyncStream<Void> empty(Consumer<List<T>> consumer) {
        return (AsyncStream<Void>) super.empty(consumer);
    }

    @Override
    public <R> AsyncStream<R> flatMap(Function<T, List<R>> function) {
        return (AsyncStream<R>) super.flatMap(function);
    }

    @Override
    public AsyncStream<T> parallelSort(Comparator<T> comparator) {
        return (AsyncStream<T>) super.parallelSort(comparator);
    }

    @Override
    public AsyncStream<T> sort(Comparator<T> comparator) {
        return (AsyncStream<T>) super.sort(comparator);
    }

    @Override
    public <R> AsyncStream<R> parallel(Function<T, R> mapper) {
        return (AsyncStream<R>) super.parallel(mapper);
    }

    @Override
    public AsyncStream<Void> forEach(Consumer<T> consumer) {
        return (AsyncStream<Void>) super.forEach(consumer);
    }

    @Override
    public AsyncStream<T> peek(Consumer<T> consumer) {
        return (AsyncStream<T>) super.peek(consumer);
    }

    @Override
    public AsyncStream<T> loop(int repetitions, Function<List<T>, AsynchronousStream<T>> stream) {
        return (AsyncStream<T>) super.loop(repetitions, stream);
    }

    @Override
    public AsyncStream<T> submit(Runnable runnable) {
        return (AsyncStream<T>) super.submit(runnable);
    }

    @Override
    public AsyncStream<T> delay(Duration duration) {
        return (AsyncStream<T>) super.delay(duration);
    }

    @Override
    public AsyncStream<T> reversed() {
        return (AsyncStream<T>) super.reversed();
    }

    @Override
    public AsyncStream<T> replace(Predicate<T> predicate, T replacement) {
        return (AsyncStream<T>) super.replace(predicate, replacement);
    }

    @Override
    public AsyncStream<T> replace(Predicate<T> predicate, Supplier<T> replacement) {
        return (AsyncStream<T>) super.replace(predicate, replacement);
    }

    @Override
    public <R> AsynchronousStream<R> repack(StreamScope scope) {
        return new AsyncStream<>(scope);
    }
}