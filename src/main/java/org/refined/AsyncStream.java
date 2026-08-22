package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
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
    public AsyncStream<T> reset() {
        return (AsyncStream<T>) super.reset();
    }

    @Override
    public AsyncStream<T> named(String id) {
        return (AsyncStream<T>) super.named(id);
    }

    @Override
    public AsyncStream<T> precondition(Consumer<StreamScope> consumer) {
        return (AsyncStream<T>) super.precondition(consumer);
    }

    @Override
    public <R> R toAbstract(Function<List<T>, R> mapper) {
        return super.toAbstract(mapper);
    }

    @Override
    public <R> R toAbstract(long ms, Function<List<T>, R> mapper) {
        return super.toAbstract(ms, mapper);
    }

    @Override
    public T[] toArray() {
        return super.toArray();
    }

    @Override
    public T[] toArray(long ms) {
        return super.toArray(ms);
    }

    @Override
    public List<T> toList() {
        return super.toList();
    }

    @Override
    public List<T> toList(long ms) {
        return super.toList(ms);
    }

    @Override
    public Collection<T> toCollection() {
        return super.toCollection();
    }

    @Override
    public Collection<T> toCollection(long ms) {
        return super.toCollection(ms);
    }

    @Override
    public AsyncStream<T> intercept(BiFunction<RuntimeException, StreamScope, List<T>> fn) {
        return (AsyncStream<T>) super.intercept(fn);
    }

    @Override
    public AsyncStream<T> intercept(BiConsumer<RuntimeException, StreamScope> consumer) {
        return (AsyncStream<T>) super.intercept(consumer);
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
    public <R> AsyncStream<R> parallel(ForkJoinPool pool, Function<T, R> mapper) {
        return (AsyncStream<R>) super.parallel(pool, mapper);
    }

    @Override
    public <R> AsyncStream<R> parallel(int threads, Function<T, R> mapper) {
        return (AsyncStream<R>) super.parallel(threads, mapper);
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
    public <R> AsyncStream<R> loop(int repetitions, Function<List<T>, AsynchronousStream<R>> stream) {
        return (AsyncStream<R>) super.loop(repetitions, stream);
    }

    @Override
    public AsyncStream<T> submit(Runnable runnable) {
        return (AsyncStream<T>) super.submit(runnable);
    }

    @Override
    public <R> AsyncStream<R> addTask(TaskNode<R> taskNode) {
        return (AsyncStream<R>) super.addTask(taskNode);
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
    public AsyncStream<T> onComplete(Consumer<List<T>> consumer) {
        return (AsyncStream<T>) super.onComplete(consumer);
    }

    @Override
    public AsyncStream<T> onStart(Runnable runnable) {
        return (AsyncStream<T>) super.onStart(runnable);
    }

    @Override
    public AsyncStream<T> onCancel(Runnable runnable) {
        return (AsyncStream<T>) super.onCancel(runnable);
    }

    @Override
    public AsyncStream<T> onComplete(Runnable runnable) {
        return (AsyncStream<T>) super.onComplete(runnable);
    }

    @Override
    public <R> AsyncStream<Void> fork(String id, Function<List<T>, AsynchronousStream<?>> function) {
        return (AsyncStream<Void>) super.fork(id, function);
    }

    @Override
    public <R> AsyncStream<Void> forkEach(Function<T, AsynchronousStream<?>> function) {
        return (AsyncStream<Void>) super.forkEach(function);
    }

    @Override
    public <R> AsyncStream<R> collect(Class<R> clazz, String... ids) {
        return (AsyncStream<R>) super.collect(clazz, ids);
    }

    @Override
    public <R> AsyncStream<R> gather(Class<R> clazz) {
        return (AsyncStream<R>) super.gather(clazz);
    }

    @Override
    public StreamScope scope() {
        return super.scope();
    }

    @Override
    <R> AsyncStream<R> repack(StreamScope scope) {
        return new AsyncStream<>(scope);
    }
}