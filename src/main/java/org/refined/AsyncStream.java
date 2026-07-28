package org.refined;

import org.refined.taskNodes.*;

import java.awt.*;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@SuppressWarnings({"unused", "unchecked", "JavadocBlankLines"})
public record AsyncStream<T>(StreamScope scope) {

    /** NOTES **
     * This will just contain some useful info about the AsyncStream<T>
     *
     * Whenever an error is thrown it will implicitly return null.
     * Ergo, the line of succession is destroyed, you can either
     * cancel if null, or supply a new [set of] value[s] if null.
     * This is true for *Any error, even if it's not thrown by the user.
     * It is recommended to handle your own errors domestically,
     * but in the exceptional case where you can't do that,
     * it will not halt the program, it will continue executing
     * with that null value previously mentioned.
     *
     * If an error is thrown, then it will return null, you will
     * have to check for both nulls & errors if an error is thrown
     * or likewise a safety check using .catchError() and .ifNull()
     *
     */

    private static final String ERROR =
        "Operations cannot be added post-start, unless enacted by a TaskNode.";

    /**
     * @param id The String ID
     * @return The AsyncStream<T>
     *     It should be known that this is your most useful utility for debugging.
     *     Always give a *Very Detailed name, for ease of use & readability.
     */
    public AsyncStream<T> named(String id) {
        check();
        scope.named(id);
        return this;
    }

    public AsyncStream() {
        this(new StreamScope());
        scope.addTask(new OfferNode<>(new Void[]{null}));
    }

    public AsyncStream(T... values) {
        this(new StreamScope());
        scope.addTask(new OfferNode<>(values));
    }

    public <X extends Collection<T>> AsyncStream(X collection) {
        this(new StreamScope());
        scope.addTask(new OfferNode<>((T[]) collection.toArray(Object[]::new)));
    }

    public AsyncStream<T> start() {
        scope.start();
        return this;
    }

    public T[] join() {
        return (T[]) scope.join();
    }

    public T[] join(long ms) {
        return (T[]) scope.join(ms);
    }

    public void cancel() {
        scope.cancel();
    }
    
    private void check() throws RuntimeException {
        if (!scope.isUnstarted()) throw new RuntimeException(ERROR);
    }
    
    public <R> AsyncStream<R> map(Function<T,R> function) {
        check();
        scope.addTask(new MapNode<>(function));
        return new AsyncStream<>(scope);
    }

   public AsyncStream<T> catchError(Consumer<RuntimeException> exceptionConsumer) {
       check();
        scope.addTask(new CatchErrorNode<T>(exceptionConsumer));
        return this;
    }

    public AsyncStream<T> catchError(Runnable runnable) {
        check();
        scope.addTask(new CatchErrorNode<T>(runnable));
        return this;
    }

    public AsyncStream<T> catchError() {
        check();
        scope.addTask(new CatchErrorNode<>(() -> {}));
        return this;
    }

    public AsyncStream<T> filter(Predicate<T> predicate) {
        check();
        scope.addTask(new FilterNode<>(predicate));
        return this;
    }

    public AsyncStream<T> delayIfAll(Predicate<T> predicate, Duration duration) {
        check();
        scope.addTask(new DelayIfAllNode<>(predicate,duration));
        return this;
    }

    public AsyncStream<T> delayIfAny(Predicate<T> predicate, Duration duration) {
        check();
        scope.addTask(new DelayIfAnyNode<>(predicate,duration));
        return this;
    }

    public AsyncStream<Void> forEach(Consumer<T> consumer) {
        check();
        scope.addTask(new ForEachNode<>(consumer));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> peek(Consumer<T> consumer) {
        check();
        scope.addTask(new PeekNode<>(consumer));
        return this;
    }

    public <R> AsyncStream<R> ifNull(Supplier<R[]> supplier) {
        check();
        scope.addTask(new IfNullNode<>(supplier));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> ifNull(R... items) {
        check();
        scope.addTask(new IfNullNode<>(items));
        return new AsyncStream<>(scope);
    }

    public <X extends Collection<R>,R> AsyncStream<R> ifNull(X item) {
        check();
        scope.addTask(new IfNullNode<>((R[]) item.toArray()));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> delay(Duration duration) {
        check();
        scope.addTask(new DelayNode<>(duration));
        return this;
    }

}
