package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class ForEachNode<T> implements TaskNode<T> {
    final Consumer<T> consumer;
    public ForEachNode(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public @NotNull Class<ForEachNode> getType() {
        return ForEachNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            for (T item : (List<T>) scope.getItems()) {
                consumer.accept(item);
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
