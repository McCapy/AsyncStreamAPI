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
    final Function<A[],AsyncStream<?>> function;

    public ForkNode(String id,Function<A[],AsyncStream<?>> function) {
        this.id = id;
        this.function = function;
    }

    @Override
    public @NotNull Class<ForkNode> getType() {
        return ForkNode.class;
    }

    @Override
    public T @NotNull [] execute(StreamScope scope) {
        try {
            AsyncStream<?> stream = function.apply((A[]) scope.getItems());
            scope.forkMap.put(id, (AsyncStream<Object>) stream);
            stream.start();
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
        }
        return null;
    }
    Function<RuntimeException,T[]> handler;
    @Override
    public Function<RuntimeException, List<T>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<T>> function) {
        this.handler = function;
    }
}
