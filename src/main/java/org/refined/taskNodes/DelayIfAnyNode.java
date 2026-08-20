package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings({"unchecked",  "rawtypes"})
public class DelayIfAnyNode<T> implements TaskNode<T> {

    final Predicate<T> predicate;
    final Duration duration;

    public DelayIfAnyNode(Predicate<T> predicate, Duration duration) {
        this.predicate = predicate;
        this.duration = duration;
    }

    @Override
    public @NotNull Class<DelayIfAnyNode> getType() {
        return DelayIfAnyNode.class;
    }

    @Override
    public T @NotNull [] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            boolean failed = true;
            for (T item : items) {
                if (predicate.test(item)) {
                    failed = false;
                    break;
                }
            }
            if (!failed) {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    if (getHandler() != null) return handler.apply(new RuntimeException(e.getMessage()));
                    return null;
                }
            }
            return items;
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return null;
        }
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
