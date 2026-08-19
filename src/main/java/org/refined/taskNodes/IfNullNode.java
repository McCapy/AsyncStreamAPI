package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class IfNullNode<T> implements TaskNode<T> {

    final Supplier<T> supplier;

    public IfNullNode(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public Class<IfNullNode> getType() {
        return IfNullNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T replacement = supplier.get();
            T[] items = (T[]) scope.getItems();
            for (int i = 0; i < items.length; i++) {
                if (items[i] == null) items[i] = replacement;
            }
            return items;
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
