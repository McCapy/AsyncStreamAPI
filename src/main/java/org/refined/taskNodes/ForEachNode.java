package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Consumer;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class ForEachNode<T> implements TaskNode<T> {
    final Consumer<T> consumer;
    public ForEachNode(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Class<ForEachNode> getType() {
        return ForEachNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            for (T item : items) {
                consumer.accept(item);
            }
        } catch (RuntimeException e) {
            scope.setError(e);
        }
        return null;
    }

}
