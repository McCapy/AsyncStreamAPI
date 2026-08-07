package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class CatchErrorNode<T> implements TaskNode<T> {

    @Override
    public Class<CatchErrorNode> getType() {
        return CatchErrorNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        return (T[]) scope.getItems();
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
