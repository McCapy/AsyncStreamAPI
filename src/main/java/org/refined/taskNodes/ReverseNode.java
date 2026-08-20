package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class ReverseNode<T> implements TaskNode<T> {
    @Override
    public @NotNull Class<?> getType() {
        return ReverseNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            return (List<T>) scope.getItems().reversed();
        } catch (RuntimeException e) {
            if (handler != null) handler.apply(e);
            return (List<T>) StreamScope.EMPTY;
        }
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
