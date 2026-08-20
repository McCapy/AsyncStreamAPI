package org.refined;

import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Function;

public interface TaskNode<T> {
    @NotNull Class<?> getType();
    @NotNull List<T> execute(StreamScope scope);
    Function<RuntimeException,List<T>> getHandler();
    void setHandler(Function<RuntimeException,List<T>> function);
}
