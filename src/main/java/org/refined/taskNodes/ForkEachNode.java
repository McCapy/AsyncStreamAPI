package org.refined.taskNodes;

import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class ForkEachNode<T,A> implements TaskNode<T> {

    final Function<A,AsyncStream<?>> function;

    public ForkEachNode(Function<A,AsyncStream<?>> function) {
        this.function = function;
    }

    @Override
    public Class<ForkEachNode> getType() {
        return ForkEachNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            Object[] items = scope.getItems();
            for (int i = 0; i < items.length; i++) {
                scope.forkMap.put(String.valueOf(i), (AsyncStream<Object>) (function.apply((A) items[i])).start());
            }
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
        }
        return null;
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
