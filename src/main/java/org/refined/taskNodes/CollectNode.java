package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CollectNode<T> implements TaskNode<T> {
    final String[] id;
    public CollectNode( String... id) {
        this.id = id;
    }

    @Override
    public @NotNull Class<CollectNode> getType() {
        return CollectNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            return (List<T>) scope.collect(id);
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
