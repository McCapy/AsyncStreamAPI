package org.refined;

import org.refined.annotation_processor.Generates;
import org.refined.async_stages.*;
import org.refined.exceptions.JoinException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

@Generates("""
            @Artificial("Default Implementation")
            public AsyncStream() {
                super();
            }
    
            @Artificial("Default Implementation")
            public AsyncStream(T... values) {
                super(values);
            }
    
            @Artificial("Default Implementation")
            public AsyncStream(Collection<T> collection) {
                super(collection);
            }
        
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> of(Collection<R> collection) {
                return new AsyncStream<>(collection);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> of(R... values) {
                return new AsyncStream<>(values);
            }
        
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<Void> ofEmpty() {
                return new AsyncStream<>();
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> yield(Consumer<RuntimeException> consumer) {
                return (AsyncStream<T>) super.yield(consumer);
            }
        
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> yield(Function<RuntimeException, List<T>> fn) {
                return (AsyncStream<T>) super.yield(fn);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> guard(Function<org.refined.AsynchronousStream<T>, org.refined.AsynchronousStream<R>> fn) {
                return (AsyncStream<R>) super.guard(fn);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> start() {
                return (AsyncStream<T>) super.start();
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> start(Executor executor) {
                return (AsyncStream<T>) super.start(executor);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> cancel() {
                return (AsyncStream<T>) super.cancel();
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> filter(Predicate<T> predicate) {
                return (AsyncStream<T>) super.filter(predicate);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> map(Function<T, R> function) {
                return (AsyncStream<R>) super.map(function);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> offer(R... items) {
                return (AsyncStream<R>) super.offer(items);
            }
        
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> offer(Collection<R> items) {
                return (AsyncStream<R>) super.offer(items);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> offer(Function<List<T>, List<T>> function) {
                return (AsyncStream<T>) super.offer(function);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<Void> empty(Runnable runnable) {
                return (AsyncStream<Void>) super.empty(runnable);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<Void> empty() {
                return (AsyncStream<Void>) super.empty();
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<Void> empty(Consumer<List<T>> consumer) {
                return (AsyncStream<Void>) super.empty(consumer);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> flatMap(Function<T, List<R>> function) {
                return (AsyncStream<R>) super.flatMap(function);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> parallelSort(Comparator<T> comparator) {
                return (AsyncStream<T>) super.parallelSort(comparator);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> sort(Comparator<T> comparator) {
                return (AsyncStream<T>) super.sort(comparator);
            }
    
            @Artificial("Default Implementation")
            @Override
            public <R> AsyncStream<R> parallel(Function<T, R> mapper) {
                return (AsyncStream<R>) super.parallel(mapper);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<Void> forEach(Consumer<T> consumer) {
                return (AsyncStream<Void>) super.forEach(consumer);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> peek(Consumer<T> consumer) {
                return (AsyncStream<T>) super.peek(consumer);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> loop(int repetitions, Function<List<T>, org.refined.AsynchronousStream<T>> stream) {
                return (AsyncStream<T>) super.loop(repetitions, stream);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> submit(Runnable runnable) {
                return (AsyncStream<T>) super.submit(runnable);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> delay(Duration duration) {
                return (AsyncStream<T>) super.delay(duration);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> reversed() {
                return (AsyncStream<T>) super.reversed();
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> replace(Predicate<T> predicate, T replacement) {
                return (AsyncStream<T>) super.replace(predicate, replacement);
            }
    
            @Artificial("Default Implementation")
            @Override
            public AsyncStream<T> replace(Predicate<T> predicate, Supplier<T> replacement) {
                return (AsyncStream<T>) super.replace(predicate, replacement);
            }
        """)
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
        if (isStarted()) throw new RuntimeException("You cannot add operations during execution, unless enacted by an org.refined.AsyncStage");
        return wrap(stage);
    }
    private void check() {
        if (isStarted())
            throw new RuntimeException(
                "You cannot add operations during execution, unless enacted by an org.refined.AsyncStage"
            );
    }
}
