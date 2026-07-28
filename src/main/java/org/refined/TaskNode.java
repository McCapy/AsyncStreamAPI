package org.refined;

public interface TaskNode<T> {
    Class<?> getType();
    T[] execute(StreamScope scope);
}
