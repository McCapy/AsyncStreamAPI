package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Consumer;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CatchErrorNode<T> implements TaskNode<T> {

    final Consumer<RuntimeException> exceptionConsumer;

    public CatchErrorNode(Consumer<RuntimeException> exceptionConsumer) {
        this.exceptionConsumer = exceptionConsumer;
    }
    public CatchErrorNode(Runnable runnable) {
        this.exceptionConsumer = _ -> runnable.run();
    }

    @Override
    public Class<CatchErrorNode> getType() {
        return CatchErrorNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            if (scope.getError() != null) {
                exceptionConsumer.accept(scope.getError());
                scope.resetError();
            }
            return (T[]) scope.getItems();
        }
        catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
