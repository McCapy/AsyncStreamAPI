package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class SubmitNode<T> implements TaskNode<T> {
    final Runnable runnable;
    public SubmitNode(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public Class<SubmitNode> getType() {
        return SubmitNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            runnable.run();
            return (T[]) scope.getItems();
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
