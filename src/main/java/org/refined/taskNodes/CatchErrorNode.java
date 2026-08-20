package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CatchErrorNode<T> implements TaskNode<T> {

    @Override
    public @NotNull Class<CatchErrorNode> getType() {
        return CatchErrorNode.class;
    }

    @Override
    public T @NotNull [] execute(StreamScope scope) {
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
