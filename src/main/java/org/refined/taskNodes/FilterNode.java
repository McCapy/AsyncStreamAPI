package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class FilterNode<T> implements TaskNode<T> {
    final Predicate<T> predicate;
    public FilterNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public @NotNull Class<FilterNode> getType() {
        return FilterNode.class;
    }

    @Override
    public @NotNull List<T> execute(StreamScope scope) {
        try {
            List<T> holder = (List<T>) scope.getItems();
            List<T> result = new ArrayList<>(holder.size());
            for (T current : holder) {
                if (!predicate.test(current)) result.add(current);
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
