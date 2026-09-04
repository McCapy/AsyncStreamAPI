package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.*;
import java.util.function.*;

/**
 * This is an abstract class which is designed to be extended, allowing the user
 * to define their own behavior--whether it be safe, or unsafe.
 * <p>
 *
 * What this class and the default implementation seek to achieve is simply,
 * a forkable stream with the ability to do many things which the
 * StreamAPI was not originally designed to do. Although, this combines
 * the same philosophy the StreamAPI had, along with features of the
 * FutureAPI which makes for an extremely flexible data/react-oriented
 * programming structure.
 * <p>
 *
 * Along with the extensibility which you don't see in either of the
 * aforementioned API's, especially the FutureAPI which is overall,
 * very hard to read and debug.
 * <p>
 *
 * This seeks to use a different philosophy towards handling exceptions.
 * Which allows for both checked-and unchecked-exceptions, which can
 * return a default value.
 * <p>
 *
 * It also features a better method of collecting results into a
 * data structure.
 *
 * @param <T> The list-cleaned type of the AsynchronousStream.
 */
@SuppressWarnings({"unchecked", "unused"})
public abstract class AsynchronousStream<T> {

    protected @NotNull StreamScope scope;

    // Status Checkers
    public final boolean isUnstarted() {
        return scope.isUnstarted();
    }
    public final boolean isStarted() {
        return scope.isStarted();
    }
    public final boolean isCancelled() {
        return scope.isCancelled();
    }
    public final boolean isCompleted() {
        return scope.isCompleted();
    }
    // Status Checkers

    // Constructors and Factory-Constructors
    public AsynchronousStream() {
        scope = new StreamScope();
        scope.wrap(new AsyncStage.OfferStage<T,Void>(items -> null));
    }
    public AsynchronousStream(@NotNull StreamScope scope) {
        this.scope = scope;
    }
    public AsynchronousStream(T... values) {
        scope = new StreamScope();
        scope.wrap(new AsyncStage.OfferStage<T,T>(_ -> Arrays.asList(values)));
    }
    public AsynchronousStream(Collection<T> collection) {
        scope = new StreamScope();
        scope.wrap(new AsyncStage.OfferStage<T,T>(item -> new ArrayList<>(collection)));
    }
    // Constructors and Factory-Constructors

    // Status Operations
    public AsynchronousStream<T> start()  {
        scope.start();
        return this;
    }
    public void cancel()  {
        scope.cancel();
    }
    // Status Operations

    // Collection Operations
    public final <R> R toAbstract(Function<List<T>,R> mapper)  {
        return mapper.apply(((List<T>) scope.join(-1)));
    }
    public final <R> R toAbstract(long ms,Function<List<T>,R> mapper)  {
        return mapper.apply(((List<T>) scope.join(ms)));
    }
    public final T[] toArray()  {
        return (T[]) scope.join(-1).toArray();
    }
    public final T[] toArray(long ms)  {
        return (T[]) scope.join(ms).toArray();
    }
    public final List<T> toList()  {
        return (List<T>) scope.join(-1);
    }
    public final List<T> toList(long ms)  {
        return (List<T>) scope.join(ms);
    }
    public final Collection<T> toCollection()  {
        return (Collection<T>) scope.join(-1);
    }
    public final Collection<T> toCollection(long ms)  {
        return (Collection<T>) scope.join(ms);
    }
    // Collection Operations

    // Error Handling
    // Error Handling

    // Transformative Operations
    public <R> AsynchronousStream<R> map(Function<T,R> function)  {
        scope.check();
        scope.wrap(new AsyncStage.MapStage<>(function));
        return  this.repack(scope);
    }
    public <R> AsynchronousStream<R> offer(R... items)  {
        scope.check();
        scope.wrap(new AsyncStage.OfferStage<>((set) -> Arrays.asList(items)));
        return this.repack(scope);
    }
    public <R> AsynchronousStream<R> offer(Collection<R> items)  {
        scope.check();
        scope.wrap(new AsyncStage.OfferStage<>((set) -> new ArrayList<>(items)));
        return this.repack(scope);
    }
    public AsynchronousStream<T> offer(Function<List<T>,List<T>> function)  {
        scope.check();
        scope.wrap(new AsyncStage.OfferStage<>(function));
        return this;
    }
    public AsynchronousStream<Void> empty(Runnable runnable)  {
        scope.check();
        scope.wrap(new AsyncStage.EmptyStage<>(runnable));
        return this.repack(scope);
    }
    public AsynchronousStream<Void> empty()  {
        scope.check();
        scope.wrap(new AsyncStage.EmptyStage<>(() -> {}));
        return this.repack(scope);
    }
    public AsynchronousStream<Void> empty(Consumer<List<T>> consumer)  {
        scope.check();
        scope.wrap(new AsyncStage.EmptyStage<>(consumer));
        return this.repack(scope);
    }
    public <R> AsynchronousStream<R> flatMap(Function<T, List<R>> function)  {
        scope.check();
        scope.wrap(new AsyncStage.FlatMapStage<>(function));
        return this.repack(scope);
    }
    public AsynchronousStream<T> parallelSort(Comparator<T> comparator)  {
        scope.check();
        scope.wrap(new AsyncStage.SortStage<>(comparator,true));
        return this;
    }
    public AsynchronousStream<T> sort(Comparator<T> comparator)  {
        scope.check();
        scope.wrap(new AsyncStage.SortStage<>(comparator,false));
        return this;
    }
    public <R> AsynchronousStream<R> parallel(Function<T,R> mapper)  {
        scope.check();
        scope.wrap(new AsyncStage.ParallelStage<>(mapper));
        return this.repack(scope);
    }
    // Transformative Operations

    // Iteration and Loops
    public AsynchronousStream<Void> forEach(Consumer<T> consumer)  {
        scope.check();
        scope.wrap(new AsyncStage.ForEachStage<>(consumer));
        return this.repack(scope);
    }
    public AsynchronousStream<T> peek(Consumer<T> consumer)  {
        scope.check();
        scope.wrap(new AsyncStage.PeekStage<>(consumer));
        return this;
    }
    public AsynchronousStream<T> loop(int repetitions,Function<List<T>,AsynchronousStream<T>> stream)  {
        scope.check();
        scope.wrap(new AsyncStage.LoopStage<>(repetitions, stream));
        return this.repack(scope);
    }
    // Iteration and Loops

    // Miscellaneous
    public AsynchronousStream<T> submit(Runnable runnable)  {
        scope.check();
        scope.wrap(new AsyncStage.SubmitStage<T,T>(runnable));
        return this;
    }
    public AsynchronousStream<T> delay(Duration duration)  {
        scope.check();
        scope.wrap(new AsyncStage.DelayStage<T,T>(duration));
        return this;
    }
    public AsynchronousStream<T> reversed()  {
        scope.check();
        scope.wrap(new AsyncStage.ReverseStage<T,T>());
        return this;
    }
    // Miscellaneous

    // Conditionals
    public AsynchronousStream<T> filter(Predicate<T> predicate)  {
        scope.check();
        scope.wrap(new AsyncStage.FilterStage<>(predicate));
        return this;
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, T replacement)  {
        scope.check();
        scope.wrap(new AsyncStage.ReplaceStage<>(predicate,() -> replacement));
        return this;
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, Supplier<T> replacement)  {
        scope.check();
        scope.wrap(new AsyncStage.ReplaceStage<>(predicate,replacement));
        return this;
    }
    // Conditionals

    // Event Operations
    // Event Operations

    // Fork operations
    // Fork Operations

    abstract <R> AsynchronousStream<R> repack(StreamScope scope);
}
