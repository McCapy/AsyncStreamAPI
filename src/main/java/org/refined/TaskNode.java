package org.refined;

import java.io.Serializable;

public interface TaskNode<T> extends  Serializable {
    Class<?> getType();
    T[] execute(StreamScope scope);
}
