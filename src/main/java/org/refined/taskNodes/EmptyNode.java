package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Consumer;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EmptyNode<T> implements TaskNode<T> {

    final Consumer<T> consumer;
    public EmptyNode(Runnable runnable) {
        this.consumer = _ -> runnable.run();
    }

    public EmptyNode(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Class<EmptyNode> getType() {
        return EmptyNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        for (T item : (T[]) scope.getItems()) consumer.accept(item);
        return null;
    }
}
