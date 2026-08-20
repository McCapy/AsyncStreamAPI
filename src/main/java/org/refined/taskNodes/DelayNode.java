package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class DelayNode<T> implements TaskNode<T> {
    final Duration duration;
    public DelayNode(Duration duration) {
        this.duration = duration;
    }

    @Override
    public @NotNull Class<DelayNode> getType() {
        return DelayNode.class;
    }

    @Override
    public T @NotNull [] execute(StreamScope scope) {
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
    public Function<RuntimeException, List<T>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<T>> function) {
        this.handler = function;
    }
}
