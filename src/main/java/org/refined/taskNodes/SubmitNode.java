package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class SubmitNode<T> implements TaskNode<T> {
    final Runnable runnable;
    public SubmitNode(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public Class<SubmitNode> getType() {
        return SubmitNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            runnable.run();
            return (T[]) scope.getItems();
        } catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
