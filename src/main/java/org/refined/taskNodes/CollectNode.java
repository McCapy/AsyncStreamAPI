package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;
import java.util.function.IntFunction;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CollectNode<T> implements TaskNode<T> {
    final String[] id;
    final IntFunction<T[]> factory;
    public CollectNode(IntFunction<T[]> fn, String... id) {
        this.factory = fn;
        this.id = id;
    }

    @Override
    public Class<CollectNode> getType() {
        return CollectNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            Object[] items = scope.collect(id);
            T[] result = factory.apply(items.length);
            for (int i = 0; i < items.length; i++) {
                result[i] = (T) items[i];
            }
            return result;
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
