package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.time.Duration;
import java.util.function.Predicate;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class DelayIfAllNode<T> implements TaskNode<T> {

    final Predicate<T> predicate;
    final Duration duration;

    public DelayIfAllNode(Predicate<T> predicate, Duration duration) {
        this.predicate = predicate;
        this.duration = duration;
    }

    @Override
    public Class<DelayIfAllNode> getType() {
        return DelayIfAllNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            boolean failed = false;
            for (T item : items) {
                if (!predicate.test(item)) {
                    failed = true;
                    break;
                }
            }
            if (failed) return items;
            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                scope.setError(new RuntimeException(e.getMessage()));
                return null;
            }
            return items;
        } catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
