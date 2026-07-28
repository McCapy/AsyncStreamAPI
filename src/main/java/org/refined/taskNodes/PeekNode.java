package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Consumer;

@SuppressWarnings({"unchecked", "rawtypes"})
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
            scope.setError(e);
            return null;
        }
    }
}
