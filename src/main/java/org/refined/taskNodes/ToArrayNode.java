package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;
import java.util.function.IntFunction;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ToArrayNode<T> implements TaskNode<T> {

    final IntFunction<T[]> factory;
    public ToArrayNode(IntFunction<T[]> factory) {
        this.factory = factory;
    }

    @Override
    public Class<ToArrayNode> getType() {
        return ToArrayNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        Object[] items = scope.getItems();
        T[] result = factory.apply(items.length);
        for (int i = 0; i < items.length; i++) {
            result[i] = (T) items[i];
        }

        return null;
    }

    Function<RuntimeException,T[]> handler;
    @Override
    public Function<RuntimeException, T[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, T[]> function) {
        handler = function;
    }
}
