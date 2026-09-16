package org.refined;

import org.refined.async_stages.*;
import org.refined.exceptions.JoinException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

@SuppressWarnings({"unchecked", "unused", "UnusedReturnValue"})
public abstract class AsynchronousStream<T> {
    protected AsyncStage<?,?> head;
    protected AsyncStage<?,?> tail;

    protected CompletableFuture<List<?>> future = CompletableFuture.completedFuture(null);

    // Constructors and Factory-Constructors
    public AsynchronousStream() {
        checkedWrap(new OfferStage<T>(ignored -> null));
    }
    public AsynchronousStream(T... values) {
        checkedWrap(new OfferStage<T>(ignored -> Arrays.asList(values)));
    }
    public AsynchronousStream(Collection<T> collection) {
        checkedWrap(new OfferStage<T>(ignored -> new ArrayList<>(collection)));
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
        future = CompletableFuture.supplyAsync(() -> head.start(this,EMPTY,false),executor);
        return this;
    }
    public AsynchronousStream<T> start() {
        if (isStarted()) return this;
        future = CompletableFuture.supplyAsync(() -> head.start(this,EMPTY,false),ForkJoinPool.commonPool());
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
        return checkedWrap(new MapStage<>(function));
    }
    public <R> AsynchronousStream<R> offer(R... items)  {
        return checkedWrap(new OfferStage<>((set) -> Arrays.asList(items)));
    }
    public <R> AsynchronousStream<R> offer(Collection<R> items)  {
        return checkedWrap(new OfferStage<>((set) -> new ArrayList<>(items)));
    }
    public AsynchronousStream<T> offer(Function<List<T>,List<T>> function)  {
        return checkedWrap(new OfferStage<>(function));
    }
    public AsynchronousStream<Void> empty(Runnable runnable)  {
        return checkedWrap(new EmptyStage<>(runnable));
    }
    public AsynchronousStream<Void> empty()  {
        return checkedWrap(new EmptyStage<>(() -> {}));
    }
    public AsynchronousStream<Void> empty(Consumer<List<T>> consumer)  {
        return checkedWrap(new EmptyStage<>(consumer));
    }
    public <R> AsynchronousStream<R> flatMap(Function<T, List<R>> function)  {
        return checkedWrap(new FlatMapStage<>(function));
    }
    public AsynchronousStream<T> parallelSort(Comparator<T> comparator)  {
        return checkedWrap(new SortStage<>(comparator,true));
    }
    public AsynchronousStream<T> sort(Comparator<T> comparator)  {
        return checkedWrap(new SortStage<>(comparator,false));
    }
    public <R> AsynchronousStream<R> parallel(Function<T,R> mapper)  {
        return checkedWrap(new ParallelStage<>(mapper));
    }
    // Transformative Operations

    // Iteration and Loops
    public AsynchronousStream<Void> forEach(Consumer<T> consumer)  {
        return checkedWrap(new ForEachStage<>(consumer));
    }
    public AsynchronousStream<T> peek(Consumer<T> consumer)  {
        return checkedWrap(new PeekStage<>(consumer));
    }
    public AsynchronousStream<T> loop(int repetitions,Function<List<T>,AsynchronousStream<T>> stream)  {
        return checkedWrap(new LoopStage<>(repetitions, stream));
    }
    // Iteration and Loops

    // Miscellaneous
    public AsynchronousStream<T> submit(Runnable runnable)  {
        return checkedWrap(new SubmitStage<T>(runnable));
    }
    public AsynchronousStream<T> delay(Duration duration)  {
        return checkedWrap(new DelayStage<T>(duration));
    }
    public AsynchronousStream<T> reversed()  {
        return checkedWrap(new ReverseStage<T>());
    }
    // Miscellaneous

    // Error Handling
    public <R> AsynchronousStream<R> guard(Function<AsynchronousStream<T>,AsynchronousStream<R>> fn) {
        return fn.apply(checkedWrap(new GuardStage<>()));
    }
    public AsynchronousStream<T> guard() {
        return checkedWrap(new GuardStage<>());
    }
    public AsynchronousStream<T> yield(Function<RuntimeException,List<T>> fn) {
        return checkedWrap(new YieldStage<>(fn));
    }
    public AsynchronousStream<T> yield(Consumer<RuntimeException> consumer) {
        return checkedWrap(new YieldStage<>(err -> {
            consumer.accept(err);
            return EMPTY;
        }));
    }
    // Error Handling

    // Conditionals
    public AsynchronousStream<T> filter(Predicate<T> predicate)  {
        return checkedWrap(new FilterStage<>(predicate));
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, T replacement)  {
        return checkedWrap(new ReplaceStage<>(predicate,() -> replacement));
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, Supplier<T> replacement)  {
        return checkedWrap(new ReplaceStage<>(predicate,replacement));
    }
    // Conditionals

    // Event Operations (surely I'll finish this eventually)
    // Event Operations


    public static final List<?> EMPTY = new ArrayList<>(1);
    <X> AsynchronousStream<X> wrap(AsyncStage<?,?> stage) {
        if (head == null) {
            head = stage;
        }
        else {
            tail.next = stage;
        }
        tail = stage;
        return (AsynchronousStream<X>) this;
    }
    <X> AsynchronousStream<X> checkedWrap(AsyncStage<?,?> stage) {
        if (isStarted()) throw new RuntimeException("You cannot add operations during execution, unless enacted by an AsyncStage");
        return wrap(stage);
    }
}
