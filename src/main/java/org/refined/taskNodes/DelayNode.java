package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.time.Duration;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class DelayNode<T> implements TaskNode<T> {
    final Duration duration;
    public DelayNode(Duration duration) {
        this.duration = duration;
    }

    @Override
    public Class<DelayNode> getType() {
        return DelayNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            if (getHandler() != null) return handler.apply(new RuntimeException(e.getMessage()));
            return null;
        }
        return (T[]) scope.getItems();
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
