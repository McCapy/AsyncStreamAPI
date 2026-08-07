package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;


@SuppressWarnings({"rawtypes" })
public class EmptyNode<T> implements TaskNode<T> {

    final Runnable runnable;
    public EmptyNode(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public Class<EmptyNode> getType() {
        return EmptyNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            runnable.run();
        }
        catch (RuntimeException e) {
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
