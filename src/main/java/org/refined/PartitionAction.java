package org.refined;

import java.util.Spliterator;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;
import java.util.function.IntFunction;

public class PartitionAction<T, R> extends RecursiveTask<R[]> {

    private final Spliterator<T> spliterator;
    private final Function<T, R> mapper;
    private final IntFunction<R[]> arrayFactory;

    public PartitionAction(Spliterator<T> spliterator,
                           Function<T, R> mapper,
                           IntFunction<R[]> arrayFactory) {
        this.spliterator = spliterator;
        this.mapper = mapper;
        this.arrayFactory = arrayFactory;
    }

    @Override
    protected R[] compute() {
        Spliterator<T> leftSplit = spliterator.trySplit();

        if (leftSplit == null) {
            return computeDirectly();
        }

        PartitionAction<T, R> leftTask = new PartitionAction<>(leftSplit, mapper, arrayFactory);
        leftTask.fork();
        R[] right = this.compute();
        R[] left = leftTask.join();
        int leftLen = left.length;
        int rightLen = right.length;
        R[] merged = arrayFactory.apply(leftLen + rightLen);
        System.arraycopy(left, 0, merged, 0, leftLen);
        System.arraycopy(right, 0, merged, leftLen, rightLen);

        return merged;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private R[] computeDirectly() {
        int size = (int) spliterator.estimateSize();
        R[] arr = arrayFactory.apply(size);

        int[] index = new int[] {0};
        while (spliterator.tryAdvance(e -> arr[index[0]++] = mapper.apply(e))) {}

        if (index[0] != size) {
            R[] trimmed = arrayFactory.apply(index[0]);
            System.arraycopy(arr, 0, trimmed, 0, index[0]);
            return trimmed;
        }

        return arr;
    }
}
