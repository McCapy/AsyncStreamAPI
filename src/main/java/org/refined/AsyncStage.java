package org.refined;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;

import java.time.Duration;
import java.util.*;
import java.util.function.*;

@SuppressWarnings({"unused", "unchecked"})
public abstract class AsyncStage<I,O> {

    protected List<Object> params;
    protected AsyncStage(Object... items) {
        this.params = new ArrayList<>(List.of(items));
    }
    public int weight() { return 2; }
    
    public abstract @NotNull List<O> execute(@NotNull StreamScope scope,@UnknownNullability List<I> items) throws RuntimeException;

    public static class CancelStage<I,O> extends AsyncStage<I,I> {

        public CancelStage(Consumer<RuntimeException> consumer) {
            super(consumer);
        }
        public CancelStage(Runnable runnable) {
            Consumer<RuntimeException> consumer = _ -> runnable.run();
            super(consumer);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            ((Consumer<RuntimeException>) params.getFirst()).accept((RuntimeException) params.get(1));
            return items;
        }

        @Override
        public int weight() {
            return 5;
        }

    }

    public static class EndStage<I,O> extends AsyncStage<I,I> {

        public EndStage() { }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            scope.taskIndex = scope.tasks.size();
            return items;
        }

        @Override
        public int weight() {
            return 4;
        }
    }

    public static class GuardStage<I,O> extends AsyncStage<I,I> {

        public GuardStage() {}

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return items;
        }

    }
    public static class YieldStage<I,O> extends AsyncStage<I,I> {

        public YieldStage(Function<RuntimeException,List<I>> fn) {
            super(fn);
        }
        public YieldStage(Consumer<RuntimeException> consumer) {
            Function<RuntimeException,List<I>> fn = err -> {
                consumer.accept(err);
                return (List<I>) StreamScope.EMPTY;
            };
            super(fn);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            RuntimeException exception = (RuntimeException) params.getLast();
            if (exception != null) {
                StringBuilder builder = new StringBuilder();
                exception.getMessage().lines()
                    .map(str -> str.replace(",","\n    "))
                    .forEachOrdered(builder::append);
                return ((Function<RuntimeException,List<I>>) params.getFirst()).apply(new RuntimeException(builder.toString()));
            }
            return items;
        }

    }

    public static class NameStage<I,O> extends AsyncStage<I,I> {

        public NameStage(String name) {
            super(name);
        }

        @Override
        public int weight() {
            return 0;
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            scope.worker = Thread.ofVirtual().name((String) params.getFirst()).unstarted(() -> {});
            return (List<I>) StreamScope.EMPTY;
        }
    }

    public static class StartStage<I,O> extends AsyncStage<I,I> {

        public StartStage(Runnable runnable) {
            super(runnable);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            ((Runnable)params.getFirst()).run();
            return (List<I>) StreamScope.EMPTY;
        }

        @Override
        public int weight() {
            return 1;
        }
    }
    public static class CompleteStage<I,O> extends AsyncStage<I,I> {

        public CompleteStage(Consumer<List<I>> consumer) {
            super(consumer);
        }

        public CompleteStage(Runnable runnable) {
            Consumer<List<I>> consumer = _ -> runnable.run();
            super(consumer);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            ((Consumer<List<I>>) params.getFirst()).accept(items);
            return (List<I>) (items.isEmpty() ? StreamScope.EMPTY : items);
        }

        @Override
        public int weight() {
            return 3;
        }
    }

    public static class CollectStage<I,O> extends AsyncStage<I,O> {

        public CollectStage(List<String> ids) {
            super(ids);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return (List<O>) ((List<String>)params.getFirst()).parallelStream()
                    .flatMap(id -> scope.forkMap.remove(id).toList().stream())
                    .toList();
        }
    }

    public static class DelayStage<I,O> extends AsyncStage<I,I> {
        public DelayStage(Duration duration) {
            super(duration);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            try {
                Thread.sleep((Duration) params.getFirst());
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
            return items;
        }
    }

    public static class EmptyStage<I,O> extends AsyncStage<I,O> {

        public EmptyStage(Runnable runnable) {
            super((Consumer<List<I>>)(_ -> runnable.run()));
        }
        public EmptyStage(Consumer<List<I>> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            ((Consumer<List<I>>) params.getFirst()).accept(items);
            return (List<O>) StreamScope.EMPTY;
        }
    }

    public static class FilterStage<I,O> extends AsyncStage<I,I> {
        public FilterStage(Predicate<I> predicate) {
            super(predicate);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            Predicate<I> predicate = (Predicate<I>) params.getFirst();
            return items.stream().filter(predicate.negate()).toList();
        }
    }

    public static class FlatMapStage<I,O> extends AsyncStage<I,O> {

        public FlatMapStage(Function<I, List<O>> function) {
            super(function);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            final Function<I,List<O>> fn = (Function<I, List<O>>) params.getFirst();
            return ((List<I>) items).stream().flatMap(val -> fn.apply(val).stream()).toList();
        }
    }

    public static class ForEachStage<I,O> extends AsyncStage<I,O> {
        public ForEachStage(Consumer<I> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            items.forEach((Consumer<I>)params.getFirst());
            return (List<O>) StreamScope.EMPTY;
        }
    }

    public static class ForkEachStage<I,O> extends AsyncStage<I,O> {

        public ForkEachStage(Function<I, AsynchronousStream<?>> function) {
            super(function);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            for (int i = 0, scopeItemsSize = items.size(); i < scopeItemsSize; i++) {
                scope.forkMap.put(String.valueOf(i), (AsyncStream<Object>) (((Function<I, AsynchronousStream<?>>) params.getFirst()).apply(items.get(i))).start());
            }
            return (List<O>) StreamScope.EMPTY;
        }
    }
    public static class ForkStage<I,O> extends AsyncStage<I,O> {

        public ForkStage(String id,Function<List<I>,AsynchronousStream<?>> function) {
            super(id,function);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            scope.forkMap.put((String) params.getFirst(), (AsynchronousStream<Object>) ((Function<List<I>,AsynchronousStream<?>>)params.get(1)).apply(items).start());
            return (List<O>) StreamScope.EMPTY;
        }
    }

    public static class GatherStage<I,O> extends AsyncStage<I,O> {

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            List<O> result = (List<O>)
                scope.forkMap.values().parallelStream()
                    .flatMap(stream -> stream.toList().stream())
                    .toList();
            scope.forkMap.clear();
            return result;
        }
    }

    public static class LoopStage<I,O> extends AsyncStage<I,I> {

        public LoopStage(int repetitions, Function<List<I>,AsynchronousStream<I>> stream) {
            super(repetitions,stream);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            int repetitions = (int) params.getFirst();
            Function<List<I>,AsynchronousStream<I>> constructor = (Function<List<I>, AsynchronousStream<I>>) params.get(1);
            for (int i = 0; i < repetitions; i++) {
                items = constructor.apply(items).toList();
            }
            return items;
        }
    }

    public static class MapStage<I,O> extends AsyncStage<I,O> {

        public MapStage(Function<I,O> function) {
            super(function);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return (items).stream().map((Function<I,O>) params.getFirst()).toList();
        }
    }

    public static class OfferStage<I,O> extends AsyncStage<I,I> {

        public OfferStage(Function<List<I>,List<I>> function) {
            super(function);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return ((Function<List<I>,List<I>>) params.getFirst()).apply(items);
        }
    }

    public static class ParallelStage<I,O> extends AsyncStage<I,O> {


        public ParallelStage(Function<I,O> mapper) {
            super(mapper);
        }

        @Override
        public @NotNull List<O> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return (items).parallelStream().map((Function<I,O>)params.getFirst()).toList();
        }
    }

    public static class PeekStage<I,O> extends AsyncStage<I,I> {

        public PeekStage(Consumer<I> consumer) {
            super(consumer);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            items.forEach((Consumer<I>) params.getFirst());
            return items;
        }
    }

    public static class ReplaceStage<I,O> extends AsyncStage<I,I> {

        public ReplaceStage(Predicate<I> predicate, Supplier<I> supplier) {
            super(predicate,supplier);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            I holder = ((Supplier<I>) params.get(1)).get();
            Predicate<I> predicate = (Predicate<I>) params.getFirst();
            return items.stream().map((item) -> predicate.test(item) ? holder : item).toList();
        }
    }

    public static class ReverseStage<I,O> extends AsyncStage<I,I> {

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            return items.reversed();
        }
    }

    public static class SortStage<I,O> extends AsyncStage<I,I> {

        public SortStage(Comparator<I> comparator, boolean parallel) {
            super(parallel,comparator);
        }

        @Override
        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            I[] res = (I[]) items.toArray();
            if ((boolean)params.getFirst()) Arrays.parallelSort(res, (Comparator<I>)params.get(1));
            else Arrays.sort(res, (Comparator<I>)params.get(1));
            return Arrays.asList(res);
        }
    }

    public static class SubmitStage<I,O> extends AsyncStage<I,I> {
        public SubmitStage(Runnable runnable) {
            super(runnable);
        }

        public @NotNull List<I> execute(@NotNull StreamScope scope, @UnknownNullability List<I> items) throws RuntimeException {
            ((Runnable)params.getFirst()).run();
            return items;
        }
    }
}
