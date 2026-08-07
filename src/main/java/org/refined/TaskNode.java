package org.refined;

import java.io.Serializable;
import java.util.function.Function;

public interface TaskNode<T> extends  Serializable {
    Class<?> getType();
    T[] execute(StreamScope scope);
    Function<RuntimeException,T[]> getHandler();
    void setHandler(Function<RuntimeException,T[]> function);
}
