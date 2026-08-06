package org.refined;

import org.refined.taskNodes.*;

import java.awt.*;
import java.time.Duration;
import java.util.Collection;
import java.util.function.*;

@SuppressWarnings({"unused", "unchecked", "JavadocBlankLines", "MethodDoesntCallSuperMethod"})
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
     * It should be known that when .fork(...) is executed it will be treated as
     * a foreign stream, that is until it has been joined, post join
     * will result in them effectively being fused together, although
     * this fusing does come with some fine print. Although nothing major.
     *
     * All values inside the AsyncStream<T> are considered Optional<T[]> inherently.
     * Meaning they *Can be null.
     *
     * It should be known that .gather(Class<T> clazz) gathers EVERY existing fork and casts
     * to the supplied `clazz` value in the form of an array, the result of this is just
     * an array containing every fork's resulting array, in the order each fork was added.
     *
     * Nextly, in regard to .collect() as well as .forkEach(), .forkEach() ID's each fork
     * from 0-n where n is the length-1 of the result of the stream.
     * As for .collect() it takes in an ID & a class type, the class type is the type which
     * is passed onwards for usage’s sake.
     *
     * Another sidenote, any type of .fork() or variation of it, implicitly
     * empties the value from the stream, regressing it into a AsyncStream<Void>
     *
     * Upcoming features:
     * - A better return value from the .join() and similar methods, which will return a version
     *   of the Optional class, although a much more refined, better version of it, which will
     *   much more effectively work for the given task, to manage null, with ease, readably.
     */

    private static final RuntimeException ERROR =
        new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskNode.");

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
        scope.addTask(new OfferNode<>((Void)null));
    }

    public AsyncStream(T... values) {
        this(new StreamScope());
        scope.addTask(new OfferNode<>(values));
    }

    public <X extends Collection<T>> AsyncStream(X collection) {
        this(new StreamScope());
        scope.addTask(new OfferNode<>((T[])collection.toArray(Object[]::new)));
    }

    public static AsyncStream<Void> empty() {
        return new AsyncStream<>();
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
        if (!scope.isUnstarted()) throw ERROR;
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

    public <R> AsyncStream<R> loop(int repetitions,AsyncStream<R> stream) {
        check();
        scope.addTask(new LoopNode<R,T>(repetitions,stream));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> submit(Runnable runnable) {
        check();
        scope.addTask(new SubmitNode<>(runnable));
        return this;
    }

    public AsyncStream<Void> empty(Runnable runnable) {
        check();
        scope.addTask(new EmptyNode<>(runnable));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> cancelIfAll(Predicate<T> predicate) {
        check();
        scope.addTask(new CancelIfAllNode<>(predicate));
        return this;
    }

    public AsyncStream<T> cancelIfAny(Predicate<T> predicate) {
        check();
        scope.addTask(new CancelIfAnyNode<>(predicate));
        return this;
    }

    public AsyncStream<T> onComplete(Consumer<T[]> consumer) {
        check();
        scope.onComplete(consumer);
        return this;
    }

    public AsyncStream<T> onComplete(Runnable runnable) {
        check();
        scope.onComplete(runnable);
        return this;
    }

    public <R> AsyncStream<R> offer(R... items) {
        check();
        scope.addTask(new OfferNode<>(items));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> offer(Collection<R> items) {
        check();
        scope.addTask(new OfferNode<>(items));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> onStart(Runnable runnable) {
        check();
        scope.onStart(runnable);
        return this;
    }

    public AsyncStream<T> onCancel(Runnable runnable) {
        check();
        scope.onCancel(runnable);
        return this;
    }

    public AsyncStream<T> onCancel(Consumer<RuntimeException> consumer) {
        check();
        scope.onCancel(consumer);
        return this;
    }

    //

    public <R> AsyncStream<Void> fork(String id, Function<T[],AsyncStream<?>> function) {
        check();
        scope.addTask(new ForkNode<R,T>(id, function));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<Void> forkEach(Function<T,AsyncStream<?>> function) {
        check();
        scope.addTask(new ForkEachNode<Void,T>(function));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> collect(Class<R> clazz,String... ids) {
        scope.addTask(new CollectNode<R>(ids));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> collect(String[] ids,Class<R> clazz) {
        scope.addTask(new CollectNode<R>(ids));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> collect(Collection<String> ids,Class<R> clazz) {
        scope.addTask(new CollectNode<R>(ids));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> gather(Class<R> clazz) {
        scope.addTask(new GatherNode<>());
        return new AsyncStream<>(scope);
    }

    //


    @Override
    protected Object clone() {
        return new AsyncStream<T>((StreamScope) this.scope.clone());
    }
}
