package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Predicate;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CancelIfAnyNode<T> implements TaskNode<T> {

    final Predicate<T> predicate;
    public CancelIfAnyNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Class<CancelIfAnyNode> getType() {
        return CancelIfAnyNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        T[] items = (T[]) scope.getItems();
        for (T item : items) {
            if (predicate.test(item))  {
                scope.cancel();
                return null;
            }
        }
        return (T[]) scope.getItems();
    }
}
