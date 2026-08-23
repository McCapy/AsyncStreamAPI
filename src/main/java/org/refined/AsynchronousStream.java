package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.*;

@SuppressWarnings({"unchecked", "unused"})
public abstract class AsynchronousStream<T> {
     protected @NotNull StreamScope scope;

    // Constructors and Factory-Constructors
    public AsynchronousStream() {
        scope = new StreamScope();
        scope.addTask(new TaskNode.OfferNode<Void>(items -> null));
    }
    public AsynchronousStream(@NotNull StreamScope scope) {
        this.scope = scope;
    }
    public AsynchronousStream(T... values) {
        scope = new StreamScope();
        scope.addTask(new TaskNode.OfferNode<T>(_ -> List.of(values)));
    }
    public AsynchronousStream(Collection<T> collection) {
        scope = new StreamScope();
        scope.addTask(new TaskNode.OfferNode<T>(item -> new ArrayList<>(collection)));
    }
    // Constructors and Factory-Constructors

    // Status Operations
    public AsynchronousStream<T> start() {
        scope.start();
        return  this;
    }
    public void cancel() {
        scope.cancel();
    }
    public AsynchronousStream<T> reset() {
        return  repack(scope.reset());
    }
    public AsynchronousStream<T> named(String id) {
        scope.check();
        scope.setName(id);
        return  this;
    }
    // Status Operations

    // Collection Operations
    public <R> R toAbstract(Function<List<T>,R> mapper) {
        return mapper.apply(((List<T>) scope.join()));
    }
    public <R> R toAbstract(long ms,Function<List<T>,R> mapper) {
        return mapper.apply(((List<T>) scope.join(ms)));
    }
    public T[] toArray() { return (T[]) scope.join().toArray(); }
    public T[] toArray(long ms) {
        return (T[]) scope.join(ms).toArray();
    }
    public List<T> toList() {
        return (List<T>) scope.join();
    }
    public List<T> toList(long ms) {
        return (List<T>) scope.join(ms);
    }
    public Collection<T> toCollection() {
        return (Collection<T>) scope.join();
    }
    public Collection<T> toCollection(long ms) {
        return (Collection<T>) scope.join(ms);
    }
    // Collection Operations

    // Error Handling
    static final RuntimeException INTERCEPTION_ERROR = new RuntimeException("Failed to intercept error--cancelled and diminished result. Solve the error.\n %s");
    public AsynchronousStream<T> intercept(BiFunction<RuntimeException,StreamScope,List<T>> fn) {
        scope.check();
        scope.injectErrorHandling((err,sc) -> {
            try {
                return fn.apply(err, sc);
            } catch (RuntimeException e) {
                RuntimeException real = new RuntimeException(INTERCEPTION_ERROR.getMessage().formatted(e.getMessage()));
                real.setStackTrace(e.getStackTrace());
                sc.cancel();
                throw real;
            }
        });
        return  this;
    }
    public AsynchronousStream<T> intercept(BiConsumer<RuntimeException,StreamScope> consumer) {
        scope.check();
        scope.injectErrorHandling((err,sc) -> {
            try {
                consumer.accept(err, sc);
                return StreamScope.EMPTY;
            } catch (RuntimeException e) {
                RuntimeException real = new RuntimeException(INTERCEPTION_ERROR.getMessage().formatted(e.getMessage()));
                real.setStackTrace(e.getStackTrace());
                sc.cancel();
                throw real;
            }
        });
        return  this;
    }
    // Error Handling

    // Transformative Operations
    public AsynchronousStream<T> filter(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new TaskNode.FilterNode<>(predicate));
        return  this;
    }
    public <R> AsynchronousStream<R> map(Function<T,R> function) {
        scope.check();
        scope.addTask(new TaskNode.MapNode<>(function));
        return  repack(scope);
    }
    public <R> AsynchronousStream<R> offer(R... items) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>((set) -> List.of(items)));
        return  repack(scope);
    }
    public <R> AsynchronousStream<R>offer(Collection<R> items) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>((set) -> new ArrayList<>(items)));
        return  repack(scope);
    }
    public AsynchronousStream<T> offer(Function<List<T>,List<T>> function) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>(function));
        return  this;
    }
    public AsynchronousStream<Void> empty(Runnable runnable) {
        scope.check();
        scope.addTask(new TaskNode.EmptyNode<>(runnable));
        return  this.repack(scope);
    }
    public AsynchronousStream<Void> empty() {
        scope.check();
        scope.addTask(new TaskNode.EmptyNode<>(() -> {}));
        return  this.repack(scope);
    }
    public AsynchronousStream<Void> empty(Consumer<List<T>> consumer) {
        scope.check();
        scope.addTask(new TaskNode.EmptyNode<>(consumer));
        return  this.repack(scope);
    }
    public <R> AsynchronousStream<R> flatMap(Function<T, List<R>> function) {
        scope.check();
        scope.addTask(new TaskNode.FlatMapNode<>(function));
        return  this.repack(scope);
    }
    public AsynchronousStream<T> parallelSort(Comparator<T> comparator) {
        scope.check();
        scope.addTask(new TaskNode.SortNode<>(comparator,true));
        return  this;
    }
    public AsynchronousStream<T> sort(Comparator<T> comparator) {
        scope.check();
        scope.addTask(new TaskNode.SortNode<>(comparator,false));
        return  this;
    }
    public <R> AsynchronousStream<R> parallel(ForkJoinPool pool, Function<T,R> mapper) {
        scope.check();
        scope.addTask(new TaskNode.ParallelNode<>(pool,mapper));
        return  repack(scope);
    }
    public <R> AsynchronousStream<R> parallel(int threads,Function<T,R> mapper) {
        scope.check();
        scope.addTask(new TaskNode.ParallelNode<>(threads,mapper));
        return repack(scope);
    }
    // Transformative Operations

    // Iteration and Loops
    public AsynchronousStream<Void> forEach(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new TaskNode.ForEachNode<>(consumer));
        return  this.repack(scope);
    }
    public AsynchronousStream<T> peek(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new TaskNode.PeekNode<>(consumer));
        return this;
    }
    public <R> AsynchronousStream<R> loop(int repetitions,Function<List<T>,AsynchronousStream<R>> stream) {
        scope.check();
        scope.addTask(new TaskNode.LoopNode<>(repetitions, stream));
        return repack(scope);
    }
    // Iteration and Loops

    // Miscellaneous
    public AsynchronousStream<T> submit(Runnable runnable) {
        scope.check();
        scope.addTask(new TaskNode.SubmitNode<>(runnable));
        return this;
    }
    public <R> AsynchronousStream<R> addTask(TaskNode<R> taskNode) {
        scope.check();
        scope.addTask(taskNode);
        return repack(scope);
    }
    public AsynchronousStream<T> delay(Duration duration) {
        scope.check();
        scope.addTask(new TaskNode.DelayNode<>(duration));
        return this;
    }
    public AsynchronousStream<T> reversed() {
        scope.check();
        scope.addTask(new TaskNode.ReverseNode<>());
        return this;
    }
    // Miscellaneous

    // Conditionals
    public AsynchronousStream<T> replace(Predicate<T> predicate, T replacement) {
        scope.check();
        scope.addTask(new TaskNode.ReplaceNode<>(predicate,() -> replacement));
        return this;
    }
    public AsynchronousStream<T> replace(Predicate<T> predicate, Supplier<T> replacement) {
        scope.check();
        scope.addTask(new TaskNode.ReplaceNode<>(predicate,replacement));
        return this;
    }
    // Conditionals

    // Event Operations
    public AsynchronousStream<T> onComplete(Consumer<List<T>> consumer) {
        scope.check();
        scope.onComplete((Consumer<List<Object>>) (Object) consumer);
        return this;
    }
    public AsynchronousStream<T> onStart(Runnable runnable) {
        scope.check();
        scope.onStart(runnable);
        return this;
    }
    public AsynchronousStream<T> onCancel(Runnable runnable) {
        scope.check();
        scope.onCancel(runnable);
        return this;
    }
    public AsynchronousStream<T> onComplete(Runnable runnable) {
        scope.check();
        scope.onComplete(runnable);
        return this;
    }
    // Event Operations

    // Fork operations
    public <R> AsynchronousStream<Void> fork(String id, Function<List<T>,AsynchronousStream<?>> function) {
        scope.check();
        scope.addTask(new TaskNode.ForkNode<R,T>(id, function));
        return repack(scope);
    }
    public <R> AsynchronousStream<Void> forkEach(Function<T,AsynchronousStream<?>> function) {
        scope.check();
        scope.addTask(new TaskNode.ForkEachNode<Void,T>(function));
        return repack(scope);
    }
    public <R> AsynchronousStream<R> collect(Class<R> clazz,List<String> ids) {
        scope.addTask(new TaskNode.CollectNode<R>(ids));
        return repack(scope);
    }
    public <R> AsynchronousStream<R> gather(Class<R> clazz) {
        scope.addTask(new TaskNode.GatherNode<R>());
        return repack(scope);
    }
    abstract <R> AsynchronousStream<R> repack(StreamScope scope);
    public StreamScope scope() {
        return this.scope;
    }
    // Fork Operations
}
