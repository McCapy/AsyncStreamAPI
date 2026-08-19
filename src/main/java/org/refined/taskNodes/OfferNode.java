package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class OfferNode<T> implements TaskNode<T> {
    final Function<T[],T[]> function;

    public OfferNode(Function<T[],T[]> function) {
        this.function = function;
    }

    @Override
    public Class<OfferNode> getType() {
        return OfferNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            return function.apply((T[]) scope.getItems());
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
