package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

@SuppressWarnings({"rawtypes", "unchecked"})
public class FilterNode<T> implements TaskNode<T> {
    final Predicate<T> predicate;
    public FilterNode(Predicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    public Class<FilterNode> getType() {
        return FilterNode.class;
    }

    @Override
    public T[] execute(StreamScope scope) {
        List<T> result = new ArrayList<>(scope.getItems().length);
        for (T item : (T[]) scope.getItems()) {
            if (predicate.test(item)) {
                result.add(item);
            }
        }
        return result.toArray((T[]) new Object[result.size()]);
    }
}
