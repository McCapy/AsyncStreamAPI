package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;
import java.util.function.IntFunction;

@SuppressWarnings({"rawtypes", "unchecked"})
public class GatherNode<T> implements TaskNode<T> {

    final IntFunction<T[]> factory;
    public GatherNode(IntFunction<T[]> fn) {
        this.factory = fn;
    }

    @Override
    public Class<GatherNode> getType() {
        return GatherNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            Object[] items = scope.gather();
            T[] result = factory.apply(items.length);
            for (int i = 0, itemsLength = items.length; i < itemsLength; i++) {
                result[i] = (T) items[i];

            }
            return result;
        } catch (RuntimeException e) {
            e.printStackTrace();
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
