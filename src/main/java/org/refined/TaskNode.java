package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.*;

// prepare your eyes for the bullshit you're about to see

@SuppressWarnings({"unused","unchecked"})
public abstract class TaskNode<T> {
    protected List<Object> params;
    protected TaskNode(Object... items) {
        this.params = List.of(items);
    }

    public abstract @NotNull List<T> execute(StreamScope scope,List<Object> items) throws RuntimeException;

    public BiFunction<RuntimeException,StreamScope,List<?>> handler() {
        return handler;
    }
    public void handler(BiFunction<RuntimeException, StreamScope, List<?>> function) {
        this.handler = function;
    }
    protected BiFunction<RuntimeException,StreamScope,List<?>> handler;

    public static class CollectNode<T> extends TaskNode<T> {
        public CollectNode(List<String> ids) {
            super(ids);
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                List<T> holder = (List<T>) items;
                List<T> result = new ArrayList<>(holder.size());
                for (T current : holder) {
                    if (!((Predicate<T>) params.getFirst()).test(current)) result.add(current);
                }
                return result;
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
        public @NotNull List<R> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                List<R> result = new ArrayList<>();
                for (T item : (List<T>) items) {
                    result.addAll(((Function<T,List<R>>)params.getFirst()).apply(item));
                }
                return result;
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                for (T item : (List<T>) items) {
                    ((Consumer<T>)params.getFirst()).accept(item);
                }
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                return (List<T>) scope.gather();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class LoopNode<R,T> extends TaskNode<R> {

        public LoopNode(int repetitions, Function<List<T>,AsynchronousStream<R>> stream) {
            super(repetitions,stream);
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                AsynchronousStream<R> stream = ((Function<List<T>,AsynchronousStream<R>>)params.get(1)).apply(((List<T>) items));
                List<Object> current = items;
                int repetitions = (int) params.getFirst();
                for (int i = 0; i < repetitions; i++) {
                    final List<Object> finalCurrent = current;
                    current = stream.reset().scope().setTask(0,new OfferNode<>(_ -> finalCurrent)).join();
                }
                return (List<R>) current;
            } catch (RuntimeException e) {
                if (handler() != null) return (List<R>) handler.apply(e,scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class MapNode<T,R> extends TaskNode<R> {

        public MapNode(Function<T,R> function) {
            super(function);
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                Function<T,R> fn = (Function<T, R>) params.getFirst();
                List<R> result = new ArrayList<>(items.size());
                Iterator<T> iterator = (Iterator<T>) items.iterator();
                while (iterator.hasNext()) {
                    result.add(fn.apply(iterator.next()));
                }
                return result;
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                return ((Function<List<T>,List<T>>) params.getFirst()).apply((List<T>) items);
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ParallelNode<T,R> extends TaskNode<R> {

        public ParallelNode(ForkJoinPool pool, Function<T,R> mapper) {
            super(mapper,pool,0);
        }

        public ParallelNode(int threads,Function<T,R> mapper) {
            super(mapper,null,threads);
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope, List<Object> items) throws RuntimeException {
            try {
                PartitionAction<T,R> action = new PartitionAction<>(((List<T>) items).spliterator(), (Function<T,R>)params.getFirst());
                ForkJoinPool pool = (ForkJoinPool) params.get(1);
                if (pool == null) {
                    try (ForkJoinPool otherPool = new ForkJoinPool((int)params.get(2))) {
                        return otherPool.invoke(action).reversed();
                    }
                }
                else {
                    return pool.invoke(action).reversed();
                }
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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

        public @NotNull List<T> execute(StreamScope scope, List<Object> items) throws RuntimeException {
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
