package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class OfferNode<T> implements TaskNode<T> {
    final Function<List<T>,List<T>> function;

    public OfferNode(Function<List<T>,List<T>> function) {
        this.function = function;
    }

    @Override
    public @NotNull Class<OfferNode> getType() {
        return OfferNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            return function.apply((List<T>) scope.getItems());
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return (List<T>) StreamScope.EMPTY;
        }
    }

    Function<RuntimeException,List<T>> handler;
    @Override
    public Function<RuntimeException, List<T>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<T>> function) {
        this.handler = function;
    }
}
