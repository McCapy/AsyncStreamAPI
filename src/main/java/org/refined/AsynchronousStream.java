package org.refined;

import org.refined.async_stages.*;
import org.refined.exceptions.JoinException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

@SuppressWarnings({"unchecked", "unused"})
public abstract class AsynchronousStream<T> {

    protected AsyncStage<?,?> head;
    protected AsyncStage<?,?> tail;

    protected CompletableFuture<List<?>> future = CompletableFuture.completedFuture(null);
    public List<AsynchronousStream<?>> forks = new ArrayList<>(5);

    // Constructors and Factory-Constructors
    public AsynchronousStream() {
        wrap(new OfferStage<T>(_ -> null));
    }
    public AsynchronousStream(T... values) {
        wrap(new OfferStage<T>(_ -> Arrays.asList(values)));
    }
    public AsynchronousStream(Collection<T> collection) {
        wrap(new OfferStage<T>(_ -> new ArrayList<>(collection)));
    }

    abstract <R> AsynchronousStream<R> of(Collection<R> collection);
    abstract <R> AsynchronousStream<R> of(R... values);
    abstract AsynchronousStream<Void> ofEmpty();
    // Constructors and Factory-Constructors

    public final boolean isStarted() {
        return future.state().equals(Future.State.RUNNING);
    }
    public final boolean isCompleted() {
        return future.isDone();
    }
    public final boolean isCancelled() {
        return future.isCancelled();
    }

    // Status Operations
    public AsynchronousStream<T> start(Executor executor)  {
        if (isStarted()) return this;
        future = CompletableFuture.supplyAsync(() -> head.start(this,EMPTY),executor);
        return this;
    }
    public AsynchronousStream<T> start() {
        if (isStarted()) return this;
        future = CompletableFuture.supplyAsync(() -> head.start(this,EMPTY),ForkJoinPool.commonPool());
        return this;
    }
    public AsynchronousStream<T> cancel() {
        if (isCancelled()) return this;
        future.cancel(true);
        return this;
    }
    // Status Operations

    // Collection Operations
    public final <R> R toAbstract(Function<List<T>,R> mapper)  {
        try {
            if (!isStarted()) start();
            return mapper.apply(((List<T>) future.get()));
        } catch (InterruptedException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final <R> R toAbstract(long ms,Function<List<T>,R> mapper) {
        try {
            if (!isStarted()) start();
            return mapper.apply(((List<T>) future.get(ms, TimeUnit.MILLISECONDS)));
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final T[] toArray()  {
        try {
            if (!isStarted()) start();
            return (T[]) future.get().toArray();
        } catch (InterruptedException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final T[] toArray(long ms) {
        try {
            if (!isStarted()) start();
            return (T[]) future.get(ms,TimeUnit.MILLISECONDS).toArray();
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final List<T> toList()  {
        try {
            if (!isStarted()) start();
            return (List<T>) future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final List<T> toList(long ms) {
        try {
            if (!isStarted()) start();
            return (List<T>) future.get(ms,TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final Collection<T> toCollection()  {
        try {
            if (!isStarted()) start();
            return (Collection<T>) future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }
    public final Collection<T> toCollection(long ms) {
        try {
            if (!isStarted()) start();
            return (Collection<T>) future.get(ms,TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            throw new JoinException(e.getMessage());
        }
    }

    // Collection Operations

    // Transformative Operations
    public <R> AsynchronousStream<R> map(Function<T,R> function)  {
        checkStarted();
        wrap(new MapStage<>(function));
        return this.repack();
    }
    public <R> AsynchronousStream<R> offer(R... items)  {
        checkStarted();
        wrap(new OfferStage<>((set) -> Arrays.asList(items)));
        return this.repack();
    }
    public <R> AsynchronousStream<R> offer(Collection<R> items)  {
        checkStarted();
        wrap(new OfferStage<>((set) -> new ArrayList<>(items)));
        return this.repack();
    }
    public AsynchronousStream<T> offer(Function<List<T>,List<T>> function)  {
        checkStarted();
        wrap(new OfferStage<>(function));
        return this;
    }
    public AsynchronousStream<Void> empty(Runnable runnable)  {
        checkStarted();
        wrap(new EmptyStage<>(runnable));
        return this.repack();
    }
    public AsynchronousStream<Void> empty()  {
        checkStarted();
        wrap(new EmptyStage<>(() -> {}));
        return this.repack();
    }
    public AsynchronousStream<Void> empty(Consumer<List<T>> consumer)  {
        checkStarted();
        wrap(new EmptyStage<>(consumer));
        return this.repack();
    }
    public <R> AsynchronousStream<R> flatMap(Function<T, List<R>> function)  {
        checkStarted();
        wrap(new FlatMapStage<>(function));
        return this.repack();
    }
    public AsynchronousStream<T> parallelSort(Comparator<T> comparator)  {
        checkStarted();
        wrap(new SortStage<>(comparator,true));
        return this;
    }
    public AsynchronousStream<T> sort(Comparator<T> comparator)  {
        checkStarted();
        wrap(new SortStage<>(comparator,false));
        return this;
    }
    public <R> AsynchronousStream<R> parallel(Function<T,R> mapper)  {
        checkStarted();
        wrap(new ParallelStage<>(mapper));
        return this.repack();
    }
    // Transformative Operations

    // Iteration and Loops
    public AsynchronousStream<Void> forEach(Consumer<T> consumer)  {
        checkStarted();
        wrap(new ForEachStage<>(consumer));
        return this.repack();
    }
    public AsynchronousStream<T> peek(Consumer<T> consumer)  {
        checkStarted();
        wrap(new PeekStage<>(consumer));
        return this;
    }
    public AsynchronousStream<T> loop(int repetitions,Function<List<T>,AsynchronousStream<T>> stream)  {
        checkStarted();
        wrap(new LoopStage<>(repetitions, stream));
        return this.repack();
    }
    // Iteration and Loops

    // Miscellaneous
    public AsynchronousStream<T> submit(Runnable runnable)  {
        checkStarted();
        wrap(new SubmitStage<T>(runnable));
        return this;
    }
    public AsynchronousStream<T> delay(Duration duration)  {
        checkStarted();
        wrap(new DelayStage<T>(duration));
        return this;
    }
    public AsynchronousStream<T> reversed()  {
        checkStarted();
        wrap(new ReverseStage<T>());
        return this;
    }
    // Miscellaneous

    // Error Handling
    public <R> AsynchronousStream<R> guard(Function<AsynchronousStream<T>,AsynchronousStream<R>> fn) {
        checkStarted();
        wrap(new GuardStage<>());
        return fn.apply(this);
    }
    public AsynchronousStream<T> yield(Function<RuntimeException,List<T>> fn) {
        checkStarted();
        wrap(new YieldStage<>(fn));
        return this;
    }
    public AsynchronousStream<T> yield(Consumer<RuntimeException> consumer) {
        checkStarted();
        wrap(new YieldStage<>(err -> {
            consumer.accept(err);
            return EMPTY;
        }));
        return repack();
    }
    // Error Handling

    // Conditionals
    public AsynchronousStream<T> filter(Predicate<T> predicate)  {
        checkStarted();
        wrap(new FilterStage<>(predicate));
        return this;
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, T replacement)  {
        checkStarted();
        wrap(new ReplaceStage<>(predicate,() -> replacement));
        return this;
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, Supplier<T> replacement)  {
        checkStarted();
        wrap(new ReplaceStage<>(predicate,replacement));
        return this;
    }
    // Conditionals

    // Event Operations
    // Event Operations

    // Fork operations
    public AsynchronousStream<Void> fork(Function<List<T>,AsynchronousStream<?>> fn) {
        checkStarted();
        wrap(new ForkStage<T,Void>(fn));
        return repack();
    }
    public <R> AsynchronousStream<R> collect(int index,Class<R> clazz) {
        checkStarted();
        wrap(new CollectStage<T,R>(index));
        return repack();
    }
    // Fork Operations

    abstract <R> AsynchronousStream<R> repack();

    public static final List<?> EMPTY = new ArrayList<>(1);
    private void wrap(AsyncStage<?,?> stage) {
        if (head == null) {
            head = stage;
        }
        else {
            tail.next = stage;
        }
        tail = stage;
    }
    private void checkStarted() {
        if (isStarted())
            throw new RuntimeException(
                "You cannot add operations during execution, unless enacted by an AsyncStage"
            );
    }
}
