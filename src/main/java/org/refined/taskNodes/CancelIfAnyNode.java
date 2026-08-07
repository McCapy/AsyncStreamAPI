package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CancelIfAnyNode<T> implements TaskNode<T> {

    final Predicate<T> predicate;
    public CancelIfAnyNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Class<CancelIfAnyNode> getType() {
        return CancelIfAnyNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            for (T item : items) {
                if (predicate.test(item)) {
                    scope.cancel();
                    return null;
                }
            }
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
