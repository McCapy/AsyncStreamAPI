package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class OfferNode<T> implements TaskNode<T> {
    final T[] objects;

    public OfferNode(T... values) {
        this.objects = values;
    }

    @Override
    public Class<OfferNode> getType() {
        return OfferNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        return objects;
    }
}
