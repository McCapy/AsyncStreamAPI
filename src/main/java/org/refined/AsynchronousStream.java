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
        scope.addTask(new TaskNode.OfferNode<T>(_ -> Arrays.asList(values)));
    }
    public AsynchronousStream(Collection<T> collection) {
        scope = new StreamScope();
        scope.addTask(new TaskNode.OfferNode<T>(item -> new ArrayList<>(collection)));
    }
    // Constructors and Factory-Constructors

    // Status Operations
    public final AsynchronousStream<T> start() {
        scope.start();
        return this;
    }
    public final void cancel() {
        scope.cancel();
    }
    public final AsynchronousStream<T> named(String id) {
        scope.check();
        scope.addTask(new TaskNode.NameNode<T>(id));
        return this;
    }
    // Status Operations

    // Collection Operations
    public final <R> R toAbstract(Function<List<T>,R> mapper) {
        return mapper.apply(((List<T>) scope.join()));
    }
    public final <R> R toAbstract(long ms,Function<List<T>,R> mapper) {
        return mapper.apply(((List<T>) scope.join(ms)));
    }
    public final T[] toArray() {
        return (T[]) scope.join().toArray();
    }
    public final T[] toArray(long ms) {
        return (T[]) scope.join(ms).toArray();
    }
    public final List<T> toList() {
        return (List<T>) scope.join();
    }
    public final List<T> toList(long ms) {
        return (List<T>) scope.join(ms);
    }
    public final Collection<T> toCollection() {
        return (Collection<T>) scope.join();
    }
    public final Collection<T> toCollection(long ms) {
        return (Collection<T>) scope.join(ms);
    }
    // Collection Operations

    // Error Handling
    static final RuntimeException INTERCEPTION_ERROR = new RuntimeException("Failed to intercept error--cancelled and diminished result. Solve the error.\n %s");
    public final AsynchronousStream<T> intercept(BiFunction<RuntimeException,StreamScope,List<T>> fn) {
        scope.check();
        scope.injectErrorHandling((err,sc) -> {
            try {
                return fn.apply(err, sc);
            } catch (RuntimeException e) {
                throw INTERCEPTION_ERROR;
            }
        });
        return this;
    }
    public final AsynchronousStream<T> intercept(BiConsumer<RuntimeException,StreamScope> consumer) {
        scope.check();
        scope.injectErrorHandling((err,sc) -> {
            try {
                consumer.accept(err, sc);
                return StreamScope.EMPTY;
            } catch (RuntimeException e) {
                throw INTERCEPTION_ERROR;
            }
        });
        return this;
    }
    // Error Handling

    // Transformative Operations
    public AsynchronousStream<T> filter(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new TaskNode.FilterNode<>(predicate));
        return this;
    }
    public <R> AsynchronousStream<R> map(Function<T,R> function) {
        scope.check();
        scope.addTask(new TaskNode.MapNode<>(function));
        return  this.repack(scope);
    }
    public <R> AsynchronousStream<R> offer(R... items) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>((set) -> List.of(items)));
        return  this.repack(scope);
    }
    public <R> AsynchronousStream<R> offer(Collection<R> items) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>((set) -> new ArrayList<>(items)));
        return  this.repack(scope);
    }
    public AsynchronousStream<T> offer(Function<List<T>,List<T>> function) {
        scope.check();
        scope.addTask(new TaskNode.OfferNode<>(function));
        return this;
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
        return this;
    }
    public AsynchronousStream<T> sort(Comparator<T> comparator) {
        scope.check();
        scope.addTask(new TaskNode.SortNode<>(comparator,false));
        return this;
    }
    public <R> AsynchronousStream<R> parallel(Function<T,R> mapper) {
        scope.check();
        scope.addTask(new TaskNode.ParallelNode<>(mapper));
        return this.repack(scope);
    }
    // Transformative Operations

    // Iteration and Loops
    public AsynchronousStream<Void> forEach(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new TaskNode.ForEachNode<>(consumer));
        return this.repack(scope);
    }
    public AsynchronousStream<T> peek(Consumer<T> consumer) {
        scope.check();
        scope.addTask(new TaskNode.PeekNode<>(consumer));
        return this;
    }
    public AsynchronousStream<T> loop(int repetitions,Function<List<T>,AsynchronousStream<T>> stream) {
        scope.check();
        scope.addTask(new TaskNode.LoopNode<>(repetitions, stream));
        return this.repack(scope);
    }
    // Iteration and Loops

    // Miscellaneous
    public AsynchronousStream<T> submit(Runnable runnable) {
        scope.check();
        scope.addTask(new TaskNode.SubmitNode<T>(runnable));
        return this;
    }
    public AsynchronousStream<T> delay(Duration duration) {
        scope.check();
        scope.addTask(new TaskNode.DelayNode<T>(duration));
        return this;
    }
    public AsynchronousStream<T> reversed() {
        scope.check();
        scope.addTask(new TaskNode.ReverseNode<T>());
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
        scope.addTask(new TaskNode.CompleteNode<>(consumer));
        return this;
    }
    public AsynchronousStream<T> onStart(Runnable runnable) {
        scope.check();
        scope.addTask(new TaskNode.StartNode<T>(runnable));
        return this;
    }
    public AsynchronousStream<T> onCancel(Runnable runnable) {
        scope.check();
        return this;
    }
    public AsynchronousStream<T> onComplete(Runnable runnable) {
        scope.check();
        scope.addTask(new TaskNode.CompleteNode<T>(runnable));
        return this;
    }
    // Event Operations

    // Fork operations
    public <R> AsynchronousStream<Void> fork(String id, Function<List<T>,AsynchronousStream<?>> function) {
        scope.check();
        scope.addTask(new TaskNode.ForkNode<R,T>(id, function));
        return this.repack(scope);
    }
    public <R> AsynchronousStream<Void> forkEach(Function<T,AsynchronousStream<?>> function) {
        scope.check();
        scope.addTask(new TaskNode.ForkEachNode<Void,T>(function));
        return this.repack(scope);
    }
    public <R> AsynchronousStream<R> collect(Class<R> clazz,List<String> ids) {
        scope.addTask(new TaskNode.CollectNode<R>(ids));
        return this.repack(scope);
    }
    public <R> AsynchronousStream<R> gather(Class<R> clazz) {
        scope.addTask(new TaskNode.GatherNode<R>());
        return this.repack(scope);
    }

    /*
       This is the default implementation of repacking,
       it just acts as a cast, by which we change the
       visual type of the stream in the users IDE.

       Although, in this case; `AsyncStream<R>`
       is a stand-in for "your" implementation,
       if you even make one, unlikely, that is.

       > ```java
       > @Override
       > public <R> AsyncStream<R> repack(StreamScope scope) {
       >     return new AsyncStream<R>(scope);
       > }
       > ```
     */

    abstract <R> AsynchronousStream<R> repack(StreamScope scope);

    public final StreamScope scope() {
        return this.scope;
    }
    // Fork Operations
}
