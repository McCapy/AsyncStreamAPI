package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class FlatMapNode<T,R> implements TaskNode<R> {

    private final Function<T, List<R>> function;
    private Function<RuntimeException, List<R>> handler;

    public FlatMapNode(Function<T, List<R>> function) {
        this.function = function;
    }

    @Override
    public @NotNull Class<FlatMapNode> getType() {
        return FlatMapNode.class;
    }

    @Override
    public @NotNull List<R> execute(StreamScope scope) {
        try {
            List<R> result = new ArrayList<>();
            for (T item : (List<T>) scope.getItems()) {
                result.addAll(function.apply(item));
            }
            return result;
        }
        catch (RuntimeException e) {
            if (handler != null) return handler.apply(e);
            return (List<R>) StreamScope.EMPTY;
        }
    }

    @Override
    public Function<RuntimeException, List<R>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<R>> function) {
        this.handler = function;
    }
}
