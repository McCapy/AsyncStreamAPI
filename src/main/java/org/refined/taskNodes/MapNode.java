package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class MapNode<T,R> implements TaskNode<R> {
    private final Function<T,R> function;

    public MapNode(Function<T,R> function) {
        this.function = function;
    }

    @Override
    public Class<MapNode> getType() {
        return MapNode.class;
    }

    @Override
    public R[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            R[] results = (R[]) new Object[items.length];

            for (int i = 0; i < items.length; i++) {
                results[i] = function.apply(items[i]);
            }

            return results;
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return null;
        }
    }

    Function<RuntimeException,R[]> handler;
    @Override
    public Function<RuntimeException, R[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException,R[]> function) {
        this.handler = function;
    }
}
