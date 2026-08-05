package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CollectNode<T> implements TaskNode<T> {
    final String id;
    public CollectNode(String id) {
        this.id = id;
    }

    @Override
    public Class<CollectNode> getType() {
        return CollectNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        Object[] items = scope.collect(id);
        scope.forkMap.remove(id);
        return (T[]) items;
    }
}
