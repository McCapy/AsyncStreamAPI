package org.refined.taskNodes;

import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class LoopNode<R,T> implements TaskNode<R> {

    final AsyncStream<R> stream;
    final int repetitions;
    public LoopNode(int repetitions, AsyncStream<R> stream) {
        this.stream = stream;
        this.repetitions = repetitions;
    }

    @Override
    public Class<LoopNode> getType() {
        return LoopNode.class;
    }

    @Override
    public R[] execute(StreamScope scope) {
        try {
            Object[] current = scope.getItems();
            for (int i = 0; i < repetitions; i++) {
                current = new AsyncStream<T>(((StreamScope) stream.scope().clone()).setTask(0, new OfferNode<>(current))).join();
            }
            return (R[]) current;
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return null;
        }
    }
    Function<RuntimeException,R[]> handler;
    @Override
    public Function<RuntimeException, R[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException,R[]> function) {
        this.handler = function;
    }
}
