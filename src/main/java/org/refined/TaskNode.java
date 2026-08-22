package org.refined;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.*;

@SuppressWarnings({"unused","unchecked"})
public abstract class TaskNode<T> {
    public abstract @NotNull List<T> execute(StreamScope scope) throws RuntimeException;
    public BiFunction<RuntimeException,StreamScope,List<?>> handler() {
        return handler;
    }
    public void handler(BiFunction<RuntimeException, StreamScope, List<?>> function) {
        this.handler = function;
    }
    protected BiFunction<RuntimeException,StreamScope,List<?>> handler;

    public static class CollectNode<T> extends TaskNode<T> {
        final String[] id;
        public CollectNode( String... id) {
            this.id = id;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                return (List<T>) scope.collect(id);
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class DelayNode<T> extends TaskNode<T> {
        final Duration duration;
        public DelayNode(Duration duration) {
            this.duration = duration;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                if (handler() != null) return (List<T>) handler.apply(new RuntimeException(e.getMessage()),scope);
                return (List<T>) StreamScope.EMPTY;
            }
            return (List<T>) scope.getItems();
        }
    }

    public static class EmptyNode<T> extends TaskNode<T> {

        final Consumer<List<T>> consumer;
        public EmptyNode(Runnable runnable) {
            this.consumer = _ -> runnable.run();
        }
        public EmptyNode(Consumer<List<T>> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                consumer.accept((List<T>) scope.getItems());
            }
            catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;

        }
    }

    public static class FilterNode<T> extends TaskNode<T> {
        final Predicate<T> predicate;
        public FilterNode(Predicate<T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                List<T> holder = (List<T>) scope.getItems();
                List<T> result = new ArrayList<>(holder.size());
                for (T current : holder) {
                    if (!predicate.test(current)) result.add(current);
                }
                return result;
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class FlatMapNode<T,R> extends TaskNode<R> {

        final Function<T, List<R>> function;

        public FlatMapNode(Function<T, List<R>> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope) throws RuntimeException {
            try {
                List<R> result = new ArrayList<>();
                for (T item : (List<T>) scope.getItems()) {
                    result.addAll(function.apply(item));
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
        final Consumer<T> consumer;
        public ForEachNode(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                for (T item : (List<T>) scope.getItems()) {
                    consumer.accept(item);
                }
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class ForkEachNode<T,A> extends TaskNode<T> {

        final Function<A,AsynchronousStream<?>> function;

        public ForkEachNode(Function<A, AsynchronousStream<?>> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                List<T> scopeItems = (List<T>) scope.getItems();
                for (int i = 0, scopeItemsSize = scopeItems.size(); i < scopeItemsSize; i++) {
                    scope.forkMap.put(String.valueOf(i), (AsyncStream<Object>) (function.apply((A) scopeItems.get(i))).start());
                }
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class ForkNode<T,A> extends TaskNode<T> {

        final String id;
        final Function<List<A>,AsynchronousStream<?>> function;

        public ForkNode(String id,Function<List<A>,AsynchronousStream<?>> function) {
            this.id = id;
            this.function = function;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                scope.forkMap.put(id, (AsynchronousStream<Object>) function.apply((List<A>) scope.getItems()).start());
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
            }
            return (List<T>) StreamScope.EMPTY;
        }
    }

    public static class GatherNode<T> extends TaskNode<T> {

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                return (List<T>) scope.gather();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class LoopNode<R,T> extends TaskNode<R> {

        final Function<List<T>, AsynchronousStream<R>> streamFunction;
        final int repetitions;
        public LoopNode(int repetitions, Function<List<T>,AsynchronousStream<R>> stream) {
            this.streamFunction = stream;
            this.repetitions = repetitions;
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope) throws RuntimeException {
            try {
                AsynchronousStream<R> stream = streamFunction.apply(((List<T>) scope.getItems()));
                List<Object>[] current = new List[1];
                current[0] = scope.getItems();
                for (int i = 0; i < repetitions; i++) {
                    current[0] = stream.reset().scope().setTask(0,new OfferNode<>(_ -> current[0])).join();
                }
                return (List<R>) current[0];
            } catch (RuntimeException e) {
                if (handler() != null) return (List<R>) handler.apply(e,scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class MapNode<T,R> extends TaskNode<R> {
        private final Function<T,R> function;

        public MapNode(Function<T,R> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope) throws RuntimeException {
            try {
                List<T> items = (List<T>) scope.getItems();
                List<R> results = new ArrayList<>(items.size());

                for (T item : items) {
                    results.add(function.apply(item));
                }

                return results;
            } catch (RuntimeException e) {
                if (handler() != null) return (List<R>) handler.apply(e, scope);
                return (List<R>) StreamScope.EMPTY;
            }
        }
    }

    public static class OfferNode<T> extends TaskNode<T> {
        final Function<List<T>,List<T>> function;

        public OfferNode(Function<List<T>,List<T>> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                return function.apply((List<T>) scope.getItems());
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ParallelNode<T,R> extends TaskNode<R> {
        final Function<T,R> mapper;
        ForkJoinPool pool;
        final int threads;

        public ParallelNode(ForkJoinPool pool, Function<T,R> mapper) {
            this.mapper = mapper;
            this.pool = pool;
            this.threads = 0;
        }

        public ParallelNode(int threads,Function<T,R> mapper) {
            this.mapper = mapper;
            this.pool = null;
            this.threads = threads;
        }

        @Override
        public @NotNull List<R> execute(StreamScope scope) throws RuntimeException {
            try {
                PartitionAction<T,R> action = new PartitionAction<>(((List<T>) scope.getItems()).spliterator(), mapper);
                if (pool == null) {
                    try (ForkJoinPool otherPool = new ForkJoinPool(threads)) {
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

        final Consumer<T> consumer;
        public PeekNode(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                List<T> items = (List<T>) scope.getItems();
                for (T item : items) {
                    consumer.accept(item);
                }
                return items;
            }
            catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ReplaceNode<T> extends TaskNode<T> {

        final Predicate<T> predicate;
        final Supplier<T> supplier;
        public ReplaceNode(Predicate<T> predicate, Supplier<T> supplier) {
            this.predicate = predicate;
            this.supplier = supplier;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                List<T> items = (List<T>) scope.getItems();
                T holder = supplier.get();
                for (int i = 0; i < items.size(); i++) {
                    if (predicate.test(items.get(i))) items.set(i, holder);
                }
                return items;
            }
            catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class ReverseNode<T> extends TaskNode<T> {

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                return (List<T>) scope.getItems().reversed();
            } catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class SortNode<T> extends TaskNode<T> {

        final Comparator<T> comparator;
        final boolean parallel;
        public SortNode(Comparator<T> comparator, boolean parallel) {
            this.comparator = comparator;
            this.parallel = parallel;
        }

        @Override
        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                T[] items = (T[]) scope.getItems().toArray();
                if (parallel) Arrays.parallelSort(items, comparator);
                else Arrays.sort(items, comparator);
                return List.of(items);
            } catch (RuntimeException e) {
                if (handler != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }

    public static class SubmitNode<T> extends TaskNode<T> {
        final Runnable runnable;
        public SubmitNode(Runnable runnable) {
            this.runnable = runnable;
        }

        public @NotNull List<T> execute(StreamScope scope) throws RuntimeException {
            try {
                runnable.run();
                return (List<T>) scope.getItems();
            } catch (RuntimeException e) {
                if (handler() != null) return (List<T>) handler.apply(e,scope);
                return (List<T>) StreamScope.EMPTY;
            }
        }
    }
}
