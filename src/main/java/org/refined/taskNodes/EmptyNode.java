package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;


@SuppressWarnings({"rawtypes" })
public class EmptyNode<T> implements TaskNode<T> {

    final Runnable runnable;
    public EmptyNode(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public Class<EmptyNode> getType() {
        return EmptyNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        runnable.run();
        return null;
    }
}
