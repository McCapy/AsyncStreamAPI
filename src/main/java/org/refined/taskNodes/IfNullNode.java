package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class IfNullNode<T> implements TaskNode<T> {

    final Supplier<T> supplier;

    public IfNullNode(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public @NotNull Class<IfNullNode> getType() {
        return IfNullNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            List<T> result = (List<T>) scope.getItems();
            T replacement = supplier.get();
            for (int i = 0; i < scope.getItems().size(); i++) {
                if (result.get(i) == null) result.set(i,replacement);
            }
            return result;
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
            return (List<T>) StreamScope.EMPTY;
        }
    }
    Function<RuntimeException,List<T>> handler;
    @Override
    public Function<RuntimeException, List<T>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<T>> function) {
        this.handler = function;
    }
}
