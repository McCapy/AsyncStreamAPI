package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

@SuppressWarnings({"rawtypes", "unchecked"})
public class GatherNode<T> implements TaskNode<T> {

    @Override
    public Class<GatherNode> getType() {
        return GatherNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            return (T[]) scope.gather();
        } catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
