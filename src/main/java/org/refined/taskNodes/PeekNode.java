package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"unchecked",  "rawtypes"})
public class PeekNode<T> implements TaskNode<T> {

    final Consumer<T> consumer;
    public PeekNode(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public @NotNull Class<PeekNode> getType() {
        return PeekNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            List<T> items = (List<T>) scope.getItems();
            for (T item : items) {
                consumer.accept(item);
            }
            return items;
        }
        catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
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
