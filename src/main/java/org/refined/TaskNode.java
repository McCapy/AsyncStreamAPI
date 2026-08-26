package org.refined;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;

import java.time.Duration;
import java.util.*;
import java.util.function.*;

@SuppressWarnings({"unused","unchecked"})
public abstract class TaskNode<T> {

    protected List<Object> params;
    protected TaskNode(Object... items) {
        this.params = Arrays.asList(items);
    }

    public int weight() { return 2; }

    public abstract @NotNull List<T> execute(@NotNull StreamScope scope,@UnknownNullability List<Object> items) throws RuntimeException;

    public final BiFunction<RuntimeException,StreamScope,List<?>> handler() {
        return handler;
    }
    public final void handler(BiFunction<RuntimeException, StreamScope, List<?>> function) {
        this.handler = function;
    }
    public BiFunction<RuntimeException,StreamScope,List<?>> handler;

    @SuppressWarnings("InstantiatingAThreadWithDefaultRunMethod")
    public static class NameNode<T> extends TaskNode<T> {

        public NameNode(String name) {
            super(name);
        }

        @Override
        public int weight() {
            return 0;
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            scope.worker = new Thread((String) params.getFirst());
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class StartNode<T> extends TaskNode<T> {

        public StartNode(Runnable runnable) {
            super(runnable);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            ((Runnable)params.getFirst()).run();
            return (List<T>) StreamScope.EMPTY;
        }

        @Override
        public int weight() {
            return 1;
        }
    }
    public static class CompleteNode<T> extends TaskNode<T> {

        public CompleteNode(Consumer<List<T>> consumer) {
            super(consumer);
        }
        public CompleteNode(Runnable runnable) {
            Consumer<List<T>> consumer = _ -> runnable.run();
            super(consumer);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            ((Consumer<List<T>>) params.getFirst()).accept((List<T>) items);
            return (List<T>) (items.isEmpty() ? StreamScope.EMPTY : items);
        }

        @Override
        public int weight() {
            return 3;
        }
    }

    public static class CollectNode<T> extends TaskNode<T> {

        public CollectNode(List<String> ids) {
            super(ids);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return (List<T>) scope.collect((List<String>) params.getFirst());
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class DelayNode<T> extends TaskNode<T> {
        public DelayNode(Duration duration) {
            super(duration);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                Thread.sleep((Duration) params.getFirst());
            } catch (InterruptedException e) {
                if (handler() != null) return (List<T>) handler.apply(new RuntimeException(e.getMessage()),scope);
                return (List<T>) StreamScope.EMPTY;
            }
            return (List<T>) items;
        }
    }

    public static class EmptyNode<T> extends TaskNode<T> {

        public EmptyNode(Runnable runnable) {
            super((Consumer<List<T>>)(_ -> runnable.run()));
        }
        public EmptyNode(Consumer<List<T>> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                ((Consumer<List<T>>) params.getFirst()).accept((List<T>) items);
            }
            catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class FilterNode<T> extends TaskNode<T> {
        public FilterNode(Predicate<T> predicate) {
            super(predicate);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                Predicate<T> predicate = (Predicate<T>) params.getFirst();
                return ((List<T>)items).stream().filter(predicate.negate()).toList();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class FlatMapNode<T,R> extends TaskNode<R> {

        public FlatMapNode(Function<T, List<R>> function) {
            super(function);
        }

        @Override
        public @NotNull List<R> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                final Function<T,List<R>> fn = (Function<T, List<R>>) params.getFirst();
                return ((List<T>) items).stream().flatMap(val -> fn.apply(val).stream()).toList();
            }
            catch (RuntimeException e) {
                if (handler != null) return (List<R>) handler.apply(e,scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class ForEachNode<T> extends TaskNode<T> {
        public ForEachNode(Consumer<T> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                ((List<T>)items).forEach((Consumer<T>)params.getFirst());
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class ForkEachNode<T,A> extends TaskNode<T> {

        public ForkEachNode(Function<A, AsynchronousStream<?>> function) {
            super(function);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                List<T> scopeItems = (List<T>) items;
                for (int i = 0, scopeItemsSize = scopeItems.size(); i < scopeItemsSize; i++) {
                    scope.forkMap.put(String.valueOf(i), (AsyncStream<Object>) (((Function<A,AsynchronousStream<?>>)params.getFirst()).apply((A) scopeItems.get(i))).start());
                }
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class ForkNode<T,A> extends TaskNode<T> {

        public ForkNode(String id,Function<List<A>,AsynchronousStream<?>> function) {
            super(id,function);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                scope.forkMap.put((String) params.getFirst(), (AsynchronousStream<Object>) ((Function<List<A>,AsynchronousStream<?>>)params.get(1)).apply((List<A>) items).start());
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class GatherNode<T> extends TaskNode<T> {

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return (List<T>) scope.gather();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class LoopNode<T> extends TaskNode<T> {

        public LoopNode(int repetitions, Function<List<T>,AsynchronousStream<T>> stream) {
            super(repetitions,stream);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                int repetitions = (int) params.getFirst();
                Function<List<T>,AsynchronousStream<T>> constructor = (Function<List<T>, AsynchronousStream<T>>) params.get(1);
                for (int i = 0; i < repetitions; i++) {
                    items = (List<Object>) constructor.apply((List<T>) items).toList();
                }
                return (List<T>) items;
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class MapNode<T,R> extends TaskNode<R> {

        public MapNode(Function<T,R> function) {
            super(function);
        }

        @Override
        public @NotNull List<R> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return ((List<T>)items).stream().map((Function<T, R>) params.getFirst()).toList();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<R>) handler.apply(e, scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class OfferNode<T> extends TaskNode<T> {

        public OfferNode(Function<List<T>,List<T>> function) {
            super(function);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return ((Function<List<T>,List<T>>) params.getFirst()).apply((List<T>) items);
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ParallelNode<T,R> extends TaskNode<R> {


        public ParallelNode(Function<T,R> mapper) {
            super(mapper);
        }

        @Override
        public @NotNull List<R> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return ((List<T>)items).parallelStream().map((Function<T,R>)params.getFirst()).toList();
            }
            catch (RuntimeException e) {
                if (handler != null) return (List<R>) handler.apply(e,scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class PeekNode<T> extends TaskNode<T> {

        public PeekNode(Consumer<T> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                Consumer<T> consumer = (Consumer<T>) params.getFirst();
                for (T item : (List<T>) items) {
                    consumer.accept(item);
                }
                return (List<T>) items;
            }
            catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ReplaceNode<T> extends TaskNode<T> {

        public ReplaceNode(Predicate<T> predicate, Supplier<T> supplier) {
            super(predicate,supplier);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                T holder = ((Supplier<T>) params.get(1)).get();
                Predicate<T> predicate = (Predicate<T>) params.getFirst();
                for (int i = 0; i < items.size(); i++) {
                    if (predicate.test((T)items.get(i))) items.set(i, holder);
                }
                return (List<T>) items;
            }
            catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ReverseNode<T> extends TaskNode<T> {

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                return (List<T>) items.reversed();
            } catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class SortNode<T> extends TaskNode<T> {

        public SortNode(Comparator<T> comparator, boolean parallel) {
            super(parallel,comparator);
        }

        @Override
        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                T[] res = (T[]) items.toArray();
                if ((boolean)params.getFirst()) Arrays.parallelSort(res, (Comparator<T>)params.get(1));
                else Arrays.sort(res, (Comparator<T>)params.get(1));
                return List.of(res);
            } catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class SubmitNode<T> extends TaskNode<T> {
        public SubmitNode(Runnable runnable) {
            super(runnable);
        }

        public @NotNull List<T> execute(@NotNull StreamScope scope, @UnknownNullability List<Object> items) throws RuntimeException {
            try {
                ((Runnable)params.getFirst()).run();
                return (List<T>) items;
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }
}
