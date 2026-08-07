package org.refined;

import org.refined.taskNodes.*;

import java.awt.*;
import java.time.Duration;
import java.util.Collection;
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
@SuppressWarnings({"unused", "unchecked", "JavadocBlankLines", "MethodDoesntCallSuperMethod", "CallToPrintStackTrace"})
public record AsyncStream<T>(StreamScope scope) {

    public AsyncStream<T> named(String id) {
        scope.check();
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
    
    public <R> AsyncStream<R> map(Function<T,R> function) {
        scope.check();
        scope.addTask(new MapNode<>(function));
        return new AsyncStream<>(scope);
    }
    
    public <R> AsyncStream<R> catchError(Function<RuntimeException,R[]> function) {
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

    public <R> AsyncStream<R> catchError(Supplier<R[]> supplier) {
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

    public AsyncStream<Void> catchError() {
        scope.check();
        scope.addTask(new CatchErrorNode<>());
        scope.injectErrorHandling(err -> {
            err.printStackTrace();
            scope.cancel();
            return new Void[]{null};
        });
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> filter(Predicate<T> predicate) {
        scope.check();
        scope.addTask(new FilterNode<>(predicate));
        return this;
    }

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

    public <R> AsyncStream<R> ifNull(Supplier<R[]> supplier) {
        scope.check();
        scope.addTask(new IfNullNode<>(supplier));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> ifNull(R... items) {
        scope.check();
        scope.addTask(new IfNullNode<>(items));
        return new AsyncStream<>(scope);
    }

    public <X extends Collection<R>,R> AsyncStream<R> ifNull(X item) {
        scope.check();
        scope.addTask(new IfNullNode<>((R[]) item.toArray()));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> delay(Duration duration) {
        scope.check();
        scope.addTask(new DelayNode<>(duration));
        return this;
    }

    public <R> AsyncStream<R> loop(int repetitions,AsyncStream<R> stream) {
        scope.check();
        scope.addTask(new LoopNode<R,T>(repetitions,stream));
        return new AsyncStream<>(scope);
    }

    public AsyncStream<T> submit(Runnable runnable) {
        scope.check();
        scope.addTask(new SubmitNode<>(runnable));
        return this;
    }

    public AsyncStream<Void> empty(Runnable runnable) {
        scope.check();
        scope.addTask(new EmptyNode<>(runnable));
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

    public AsyncStream<T> onComplete(Consumer<T[]> consumer) {
        scope.check();
        scope.onComplete((Consumer<Object[]>) (Object) consumer);
        return this;
    }

    public AsyncStream<T> onComplete(Runnable runnable) {
        scope.check();
        scope.onComplete(runnable);
        return this;
    }

    public <R> AsyncStream<R> offer(R... items) {
        scope.check();
        scope.addTask(new OfferNode<>(items));
        return new AsyncStream<>(scope);
    }

    public <R> AsyncStream<R> offer(Collection<R> items) {
        scope.check();
        scope.addTask(new OfferNode<>(items));
        return new AsyncStream<>(scope);
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

    //

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
