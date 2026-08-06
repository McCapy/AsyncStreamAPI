package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.Collection;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CollectNode<T> implements TaskNode<T> {
    final String[] id;
    public CollectNode(String... id) {
        this.id = id;
    }

    public CollectNode(Collection<String> id) {
        this.id = id.toArray(String[]::new);
    }

    @Override
    public Class<CollectNode> getType() {
        return CollectNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        Object[] items = scope.collect(id);
        for (String string : id) {
            scope.forkMap.remove(string);
        }
        return (T[]) items;
    }
}
