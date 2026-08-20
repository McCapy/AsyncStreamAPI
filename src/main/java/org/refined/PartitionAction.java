package org.refined;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PartitionAction<T, R> extends RecursiveTask<List<R>> {

    private final Spliterator<T> spliterator;
    private final Function<T, R> mapper;

    public PartitionAction(Spliterator<T> spliterator, Function<T, R> mapper) {
        this.spliterator = spliterator;
        this.mapper = mapper;
    }

    @Override
    protected List<R> compute() {
        Spliterator<T> leftSplit = spliterator.trySplit();
        if (leftSplit == null) {
            return computeDirectly();
        }

        List<R> result = this.compute();
        result.addAll(new PartitionAction<>(leftSplit, mapper).fork().join());
        return result;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private List<R> computeDirectly() {
        List<R> result = new ArrayList<>((int) spliterator.estimateSize());
        while (spliterator.tryAdvance(e -> result.add(mapper.apply(e)))) {}
        return result;
    }

}
