package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ReplaceNode<T> implements TaskNode<T> {

    final Predicate<T> predicate;
    final Supplier<T> supplier;
    public ReplaceNode(Predicate<T> predicate, Supplier<T> supplier) {
        this.predicate = predicate;
        this.supplier = supplier;
    }

    @Override
    public @NotNull Class<ReplaceNode> getType() {
        return ReplaceNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            List<T> items = (List<T>) scope.getItems();
            T holder = supplier.get();
            for (int i = 0; i < items.size(); i++) {
                if (predicate.test(items.get(i))) items.set(i, holder);
            }
            return items;
        }
        catch (RuntimeException e) {
            if (handler != null) handler.apply(e);
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
