package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.Arrays;
import java.util.Comparator;
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
    public Class<SortNode> getType() {
        return SortNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            if (parallel) Arrays.parallelSort(items, comparator);
            else Arrays.sort(items, comparator);
            return items;
        } catch (RuntimeException e) {
            if (handler != null) return handler.apply(e);
            return null;
        }
    }

    Function<RuntimeException,T[]> handler;
    @Override
    public Function<RuntimeException, T[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, T[]> function) {
        this.handler = function;
    }
}
