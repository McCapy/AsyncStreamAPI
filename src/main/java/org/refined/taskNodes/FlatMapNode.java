package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class FlatMapNode<T,R> implements TaskNode<R> {

    private final Function<T, R[]> function;
    private Function<RuntimeException, R[]> handler;

    public FlatMapNode(Function<T, R[]> function) {
        this.function = function;
    }

    @Override
    public @NotNull Class<FlatMapNode> getType() {
        return FlatMapNode.class;
    }

    @Override
    public R @NotNull [] execute(StreamScope scope) {
        try {
            List<R> result = new ArrayList<>();
            for (T item : (T[]) scope.getItems()) {
                Collections.addAll(result, function.apply(item));
            }
            return (R[]) result.toArray();
        }
        catch (RuntimeException e) {
            if (handler != null) return handler.apply(e);
            return null;
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
