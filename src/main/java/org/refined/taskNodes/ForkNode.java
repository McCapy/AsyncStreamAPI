package org.refined.taskNodes;

import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ForkNode<T,A> implements TaskNode<T> {

    final String id;
    final Function<A[],AsyncStream<?>> function;

    public ForkNode(String id,Function<A[],AsyncStream<?>> function) {
        this.id = id;
        this.function = function;
    }

    @Override
    public Class<ForkNode> getType() {
        return ForkNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            AsyncStream<?> stream = function.apply((A[]) scope.getItems());
            scope.forkMap.put(id, (AsyncStream<Object>) stream);
            stream.start();
        } catch (RuntimeException e) {
            scope.setError(e);
        }
        return null;
    }
}
