package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class MapNode<T,R> implements TaskNode<R> {
    private final Function<T,R> function;

    public MapNode(Function<T,R> function) {
        this.function = function;
    }

    @Override
    public @NotNull Class<MapNode> getType() {
        return MapNode.class;
    }

    @Override
    public @NotNull List<R> execute(StreamScope scope) {
        try {
            List<T> items = (List<T>) scope.getItems();
            List<R> results = new ArrayList<>(items.size());

            for (T item : items) {
                results.add(function.apply(item));
            }

            return results;
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return (List<R>) StreamScope.EMPTY;
        }
    }

    Function<RuntimeException,List<R>> handler;
    @Override
    public Function<RuntimeException, List<R>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<R>> function) {
        this.handler = function;
    }
}
