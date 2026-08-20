package org.refined;

import org.refined.taskNodes.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.*;

/**
 * A class that seeks to simplify Asynchronous/Reactive programming via offering a much more rich, and
 * syntactically basic & extensible DSL, this includes your own syntax, you're able to make your own
 * TaskNode's (a TaskNode is just a class which is held and executed) by implementing the TaskNode<T>
 * interface, and giving your class the generic, <T> or any other generics you may need.
 *
 * These custom TaskNode's can be added via AsyncStream<?>.scope().addTask(), there are also variations
 * of this, although; it's considered taboo to do this, as this is called unsafe tasking, you should
 * always use StreamScope.check(); and repackage your AsyncStream<T> via new AsyncStream<>(StreamScope)
 * which helps java determine the type, these methods are recommended to be in a wrapper class, which
 * holds your custom methods, as well as the AsyncStream<T> instance.
 *
 *
 * @param scope This is the scope of the stream, it is essentially the internals that dictate how the stream is to act.
 * @param <T> This is the array-cleaned type of the AsyncStream, T is internally treated as Object[] which is cast
 *           to/from T/T[]
 */

@SuppressWarnings({"unused", "unchecked", "JavadocBlankLines", "CallToPrintStackTrace"})
public record AsyncStream<T>(StreamScope scope) {

    // Constructors and Factory-Constructors
    public AsyncStream() {
        this(new StreamScope());
        scope.addTask(new OfferNode<Void>(items -> null));
    }
    public AsyncStream(T... values) {
        this(new StreamScope());
        scope.addTask(new OfferNode<T>(items -> List.of(values)));
    }
    public AsyncStream(Collection<T> collection) {
        this(new StreamScope());
        scope.addTask(new OfferNode<T>(item -> new ArrayList<>(collection)));
    }
    public static <R> AsyncStream<R> of(R... values) {
        return new AsyncStream<>(values);
    }
    public static <R> AsyncStream<R> of(Collection<R> values) {
        return new AsyncStream<>(values);
    }
    public static AsyncStream<Void> ofEmpty() {
        return new AsyncStream<>();
    }
    // Constructors and Factory-Constructors

    // Status Operations
    public AsyncStream<T> start() {
        scope.start();
        return this;
    }
    public void cancel() {
        scope.cancel();
    }
    public AsyncStream<T> reset() {
        return new AsyncStream<>(this.scope.reset());
    }
    public AsyncStream<T> named(String id) {
        scope.check();
        scope.setName(id);
        return this;
    }
    public T[] toArray(IntFunction<T[]> arr) {
        return (T[]) scope.join().toArray();
    }
    public T[] toArray(IntFunction<T[]> arr,long ms) {
        return (T[]) scope.join(ms).toArray();
    }
    // Status Operations

    // Error Handling
    public <R> AsyncStream<R> catchError(Function<RuntimeException,List<R>> function) {
        scope.check();
        scope.addTask(new CatchErrorNode<R>());
        scope.injectErrorHandling(function);
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> catchError(Consumer<RuntimeException> exceptionConsumer) {
        scope.check();
        scope.addTask(new CatchErrorNode<>());
        scope.injectErrorHandling(err -> {
           exceptionConsumer.accept(err);
           return null;
       });
       return this;
    }
    public <R> AsyncStream<R> catchError(Supplier<List<R>> supplier) {
        scope.check();
        scope.addTask(new CatchErrorNode<>());
        scope.injectErrorHandling(err -> supplier.get());
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> catchError(Runnable runnable) {
        scope.check();
        scope.addTask(new CatchErrorNode<>());
        scope.injectErrorHandling(err -> {
            runnable.run();
            return null;
        });
        return this;
    }
    public AsyncStream<T> catchError() {
        scope.check();
        scope.addTask(new CatchErrorNode<>());
        scope.injectErrorHandling(err -> {
            err.printStackTrace();
            scope.cancel();
            return null;
        });
        return new AsyncStream<>(scope);
    }
    // Error Handling

    // Transformative Operations
    public AsyncStream<T> filter(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new FilterNode<>(predicate));
        return this;
    }
    public <R> AsyncStream<R> map(Function<T,R> function) {
        scope.check();
        scope.addTask(new MapNode<>(function));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> offer(R... items) {
        scope.check();
        scope.addTask(new OfferNode<>((set) -> List.of(items)));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> offer(Collection<R> items) {
        scope.check();
        scope.addTask(new OfferNode<>((set) -> new ArrayList<>(items)));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> offer(Function<List<T>,List<T>> function) {
        scope.check();
        scope.addTask(new OfferNode<>(function));
        return this;
    }
    public AsyncStream<Void> empty(Runnable runnable) {
        scope.check();
        scope.addTask(new EmptyNode<>(runnable));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> flatMap(Function<T, R[]> function) {
        scope.check();
        scope.addTask(new FlatMapNode<>(function));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> parallelSort(Comparator<T> comparator) {
        scope.check();
        scope.addTask(new SortNode<>(comparator,true));
        return this;
    }
    public AsyncStream<T> sort(Comparator<T> comparator) {
        scope.check();
        scope.addTask(new SortNode<>(comparator,false));
        return this;
    }
    public <R> AsyncStream<R> parallel(IntFunction<R[]> func,ForkJoinPool pool,Function<T,R> mapper) {
        scope.check();
        scope.addTask(new ParallelNode<>(pool,mapper));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> parallel(IntFunction<R[]> func,int threads,Function<T,R> mapper) {
        scope.check();
        scope.addTask(new ParallelNode<>(threads,mapper));
        return new AsyncStream<>(scope);
    }
    // Transformative Operations

    // Iteration and Loops
    public AsyncStream<Void> forEach(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new ForEachNode<>(consumer));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> peek(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new PeekNode<>(consumer));
        return this;
    }
    public <R> AsyncStream<R> loop(int repetitions,Function<List<T>,AsyncStream<R>> stream) {
        scope.check();
        scope.addTask(new LoopNode<>(repetitions, stream));
        return new AsyncStream<>(scope);
    }
    // Iteration and Loops

    // Delay and Delay Conditionals
    public AsyncStream<T> delayIfAll(Predicate<T> predicate, Duration duration) {
        scope.check();
        scope.addTask(new DelayIfAllNode<>(predicate,duration));
        return this;
    }
    public AsyncStream<T> delayIfAny(Predicate<T> predicate, Duration duration) {
        scope.check();
        scope.addTask(new DelayIfAnyNode<>(predicate,duration));
        return this;
    }
    public AsyncStream<T> delay(Duration duration) {
        scope.check();
        scope.addTask(new DelayNode<>(duration));
        return this;
    }
    // Delay and Delay Conditionals

    // Miscellaneous
    public <R> AsyncStream<R> cast(IntFunction<R[]> arrayFactory) {
        scope.check();
        scope.addTask(new MapNode<T,R>(item -> (R)item));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> cast(Class<R> clazz) {
        scope.check();
        scope.addTask(new MapNode<T,R>(item -> (R)item));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> submit(Runnable runnable) {
        scope.check();
        scope.addTask(new SubmitNode<>(runnable));
        return this;
    }
    public <R> AsyncStream<R> addTask(TaskNode<R> taskNode) {
        scope.check();
        scope.addTask(taskNode);
        return new AsyncStream<>(scope);
    }
    // Miscellaneous

    // Conditionals
    public AsyncStream<T> ifNull(Supplier<T> supplier) {
        scope.check();
        scope.addTask(new IfNullNode<>(supplier));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> ifNull(T item) {
        scope.check();
        scope.addTask(new IfNullNode<>(() -> item));
        return new AsyncStream<>(scope);
    }
    public AsyncStream<T> cancelIfAll(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new CancelIfAllNode<>(predicate));
        return this;
    }
    public AsyncStream<T> cancelIfAny(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new CancelIfAnyNode<>(predicate));
        return this;
    }
    // Conditionals

    // Event Operations
    public AsyncStream<T> onComplete(Consumer<List<T>> consumer) {
        scope.check();
        scope.onComplete((Consumer<List<Object>>) (Object) consumer);
        return this;
    }
    public AsyncStream<T> onStart(Runnable runnable) {
        scope.check();
        scope.onStart(runnable);
        return this;
    }
    public AsyncStream<T> onCancel(Runnable runnable) {
        scope.check();
        scope.onCancel(runnable);
        return this;
    }
    public AsyncStream<T> onComplete(Runnable runnable) {
        scope.check();
        scope.onComplete(runnable);
        return this;
    }
    // Event Operations

    // Fork operations
    public <R> AsyncStream<Void> fork(String id, Function<T[],AsyncStream<?>> function) {
        scope.check();
        scope.addTask(new ForkNode<R,T>(id, function));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<Void> forkEach(Function<T,AsyncStream<?>> function) {
        scope.check();
        scope.addTask(new ForkEachNode<Void,T>(function));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> collect(Class<R> clazz,String... ids) {
        scope.addTask(new CollectNode<R>(ids));
        return new AsyncStream<>(scope);
    }
    public <R> AsyncStream<R> gather(Class<R> clazz) {
        scope.addTask(new GatherNode<R>());
        return new AsyncStream<>(scope);
    }
    // Fork Operations
}