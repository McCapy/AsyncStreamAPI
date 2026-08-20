package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SortNode<T> implements TaskNode<T> {

    final Comparator<T> comparator;
    final boolean parallel;
    public SortNode(Comparator<T> comparator, boolean parallel) {
        this.comparator = comparator;
        this.parallel = parallel;
    }

    @Override
    public @NotNull Class<SortNode> getType() {
        return SortNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems().toArray();
            if (parallel) Arrays.parallelSort(items, comparator);
            else Arrays.sort(items, comparator);
            return List.of(items);
        } catch (RuntimeException e) {
            if (handler != null) return handler.apply(e);
            return (List<T>) StreamScope.EMPTY;
        }
    }

    Function<RuntimeException,List<T>> handler;
    @Override
    public Function<RuntimeException, List<T>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<T>> function) {
        this.handler = function;
    }
}
