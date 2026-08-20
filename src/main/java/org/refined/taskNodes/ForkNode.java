package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class ForkNode<T,A> implements TaskNode<T> {

    final String id;
    final Function<List<A>,AsyncStream<?>> function;

    public ForkNode(String id,Function<List<A>,AsyncStream<?>> function) {
        this.id = id;
        this.function = function;
    }

    @Override
    public @NotNull Class<ForkNode> getType() {
        return ForkNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            scope.forkMap.put(id, (AsyncStream<Object>) function.apply((List<A>) scope.getItems()).start());
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
        }
        return (List<T>) StreamScope.EMPTY;
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
