package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

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
            if (getHandler() != null) return handler.apply(e);
            return null;
        }
    }
    Function<RuntimeException,T[]> handler;
    @Override
    public Function<RuntimeException, T[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException,T[]> function) {
        this.handler = function;
    }
}
