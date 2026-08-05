package org.refined.taskNodes;

import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ForkEachNode<T> implements TaskNode<T> {

    final Class<T> expectedClass;
    final AsyncStream<T> stream;

    public ForkEachNode(Class<T> clazz,AsyncStream<T> stream) {
        this.expectedClass = clazz;
        this.stream = stream;
    }

    @Override
    public Class<ForkEachNode> getType() {
        return ForkEachNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        Object[] items = scope.getItems();
        StreamScope holder = stream.scope();
        for (int i = 0; i < items.length; i++) {
            holder.setTask(0,new OfferNode<>(items[i]));
            scope.identityMap.put(String.valueOf(i), (Map.Entry<AsyncStream<Object>, Class<Object>>) (Object) Map.entry(new AsyncStream<T>((StreamScope) holder.clone()).start(),expectedClass));
        }
        return null;
    }
}
