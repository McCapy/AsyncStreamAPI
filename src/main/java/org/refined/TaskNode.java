package org.refined;

import java.util.function.Function;

public interface TaskNode<T>  {
    Class<?> getType();
    T[] execute(StreamScope scope);
    Function<RuntimeException,T[]> getHandler();
    void setHandler(Function<RuntimeException,T[]> function);
}
