package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.function.Supplier;

@SuppressWarnings({"rawtypes", "unchecked"})
public class IfNullNode<T> implements TaskNode<T> {

    final Supplier<T[]> supplier;

    public IfNullNode(Supplier<T[]> supplier) {
        this.supplier = supplier;
    }

    @SafeVarargs
    public IfNullNode(T... items) {
        this.supplier = () -> items;
    }

    @Override
    public Class<IfNullNode> getType() {
        return IfNullNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        try {
            T[] items = (T[]) scope.getItems();
            if (items == null || items instanceof Void[]) {
                return supplier.get();
            }
            return items;
        } catch (RuntimeException e) {
            scope.setError(e);
            return null;
        }
    }
}
