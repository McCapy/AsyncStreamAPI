package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"unchecked",  "rawtypes"})
public class PeekNode<T> implements TaskNode<T> {

    final Consumer<T> consumer;
    public PeekNode(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Class<PeekNode> getType() {
        return PeekNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            for (T item : items) {
                consumer.accept(item);
            }
            return items;
        }
        catch (RuntimeException e) {
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
