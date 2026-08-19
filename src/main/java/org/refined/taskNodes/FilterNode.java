package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class FilterNode<T> implements TaskNode<T> {
    final Predicate<T> predicate;
    public FilterNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Class<FilterNode> getType() {
        return FilterNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            List<T> result = new ArrayList<>(items.length);
            for (T item : items) {
                if (predicate.test(item)) {
                    result.add(item);
                }
            }
            if (result.isEmpty()) {
                return (T[]) java.lang.reflect.Array.newInstance(
                        items.getClass().getComponentType(), 0);
            }
            T[] arr = (T[]) java.lang.reflect.Array.newInstance(
                    items.getClass().getComponentType(), result.size());
            return result.toArray(arr);
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return null;
        }
    }
    Function<RuntimeException,T[]> handler;
    @Override
    public Function<RuntimeException, T[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException,T[]> function) {
        this.handler = function;
    }
}
