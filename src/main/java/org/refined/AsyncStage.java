package org.refined;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;

import java.time.Duration;
import java.util.*;
import java.util.function.*;

@SuppressWarnings({"unused", "unchecked"})
public abstract class AsyncStage<I,O> {

    public abstract @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException;
    protected AsyncStage<?,?> next;
    final List<?> start(StreamScope scope, List<?> items) {
        System.out.println(items);
        System.out.println("started");
        return advance(scope,compute(scope,items));
    }
    final List<?> advance(StreamScope scope,List<?> items) {
        System.out.println(items);
        System.out.println("advancing");
        if (next != null) {
            return next.start(scope,items);
        }
        else return items;
    }

    public static class DelayStage<I,O> extends AsyncStage<I,I> {
        final Duration duration;
        public DelayStage(Duration duration) {
            this.duration = duration;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage());
            }
            return items;
        }
    }

    public static class EmptyStage<I,O> extends AsyncStage<I,O> {
        final Consumer<List<I>> consumer;
        public EmptyStage(Runnable runnable) {
            this.consumer = (_ -> runnable.run());
        }
        public EmptyStage(Consumer<List<I>> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            consumer.accept((List<I>) items);
            return StreamScope.EMPTY;
        }
    }

    public static class FilterStage<I,O> extends AsyncStage<I,I> {
        final Predicate<I> predicate;
        public FilterStage(Predicate<I> predicate) {
            this.predicate = predicate;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return ((List<I>) items).stream().filter(predicate.negate()).toList();
        }
    }

    public static class FlatMapStage<I,O> extends AsyncStage<I,O> {
        final Function<I, List<O>> function;
        public FlatMapStage(Function<I, List<O>> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return ((List<I>)items).stream().flatMap(val -> function.apply(val).stream()).toList();
        }
    }

    public static class ForEachStage<I,O> extends AsyncStage<I,O> {
        final Consumer<I> consumer;
        public ForEachStage(Consumer<I> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            ((List<I>)items).forEach(consumer);
            return StreamScope.EMPTY;
        }
    }

    public static class LoopStage<I,O> extends AsyncStage<I,I> {

        final int repetitions;
        final Function<List<I>,AsynchronousStream<I>> streamFunction;
        public LoopStage(int repetitions, Function<List<I>,AsynchronousStream<I>> stream) {
            this.repetitions = repetitions;
            this.streamFunction = stream;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            for (int i = 0; i < repetitions; i++) {
                items = streamFunction.apply((List<I>) items).toList();
            }
            return items;
        }
    }

    public static class MapStage<I,O> extends AsyncStage<I,O> {

        final Function<I,O> function;
        public MapStage(Function<I,O> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return ((List<I>)items).stream().map(function).toList();
        }
    }

    public static class OfferStage<I,O> extends AsyncStage<I,I> {

        final Function<List<I>,List<I>> function;
        public OfferStage(Function<List<I>,List<I>> function) {
            this.function = function;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return function.apply((List<I>) items);
        }
    }

    public static class ParallelStage<I,O> extends AsyncStage<I,O> {

        final Function<I,O> function;
        public ParallelStage(Function<I,O> mapper) {
            this.function = mapper;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return ((List<I>)items).parallelStream().map(function).toList();
        }
    }

    public static class PeekStage<I,O> extends AsyncStage<I,I> {

        final Consumer<I> consumer;

        public PeekStage(Consumer<I> consumer) {
            this.consumer = consumer;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            ((List<I>)items).forEach(consumer);
            return items;
        }
    }

    public static class ReplaceStage<I,O> extends AsyncStage<I,I> {

        final Predicate<I> predicate;
        final Supplier<I> supplier;

        public ReplaceStage(Predicate<I> predicate, Supplier<I> supplier) {
            this.predicate = predicate;
            this.supplier = supplier;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            I holder = supplier.get();
            return ((List<I>)items).stream().map((item) -> predicate.test(item) ? holder : item).toList();
        }
    }

    public static class ReverseStage<I,O> extends AsyncStage<I,I> {

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            return items.reversed();
        }
    }

    public static class SortStage<I,O> extends AsyncStage<I,I> {

        final Comparator<I> comparator;
        final boolean parallel;

        public SortStage(Comparator<I> comparator, boolean parallel) {
            this.comparator = comparator;
            this.parallel = parallel;
        }

        @Override
        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            I[] res = (I[]) items.toArray();
            if (parallel) Arrays.parallelSort(res, comparator);
            else Arrays.sort(res, comparator);
            return Arrays.asList(res);
        }
    }

    public static class SubmitStage<I,O> extends AsyncStage<I,I> {
        final Runnable runnable;
        public SubmitStage(Runnable runnable) {
            this.runnable = runnable;
        }

        public @NotNull List<?> compute(@NotNull StreamScope scope, @UnknownNullability List<?> items) throws RuntimeException {
            runnable.run();
            return items;
        }
    }
}
