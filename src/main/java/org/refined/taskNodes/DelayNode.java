package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.time.Duration;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class DelayNode<T> implements TaskNode<T> {
    final Duration duration;
    public DelayNode(Duration duration) {
        this.duration = duration;
    }

    @Override
    public Class<DelayNode> getType() {
        return DelayNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            scope.setError(new RuntimeException(e.getMessage()));
            return null;
        }
        return (T[]) scope.getItems();
    }
}
