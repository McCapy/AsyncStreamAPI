package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class OfferNode<T> implements TaskNode<T> {
    final T[] objects;

    public OfferNode(T... values) {
        this.objects = values;
    }

    @Override
    public Class<OfferNode> getType() {
        return OfferNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            return objects;
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
