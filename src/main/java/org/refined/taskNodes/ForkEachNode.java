package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class ForkEachNode<T,A> implements TaskNode<T> {

    final Function<A,AsyncStream<?>> function;

    public ForkEachNode(Function<A,AsyncStream<?>> function) {
        this.function = function;
    }

    @Override
    public @NotNull Class<ForkEachNode> getType() {
        return ForkEachNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            List<T> scopeItems = (List<T>) scope.getItems();
            for (int i = 0, scopeItemsSize = scopeItems.size(); i < scopeItemsSize; i++) {
                scope.forkMap.put(String.valueOf(i), (AsyncStream<Object>) (function.apply((A) scopeItems.get(i))).start());
            }
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
        }
        return (List<T>) StreamScope.EMPTY;
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
