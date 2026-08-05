package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Predicate;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CancelIfAllNode<T> implements TaskNode<T> {
    final Predicate<T> predicate;
    public CancelIfAllNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }
    @Override
    public Class<CancelIfAllNode> getType() {
        return CancelIfAllNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            for (T item : items) {
                if (!predicate.test(item)) {
                    scope.cancel();
                    return null;
                }
            }
            return items;
        } catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
