package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.StreamScope;
import org.refined.TaskNode;
import org.refined.PartitionAction;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ParallelNode<T,R> implements TaskNode<R> {
    final Function<T,R> mapper;
    ForkJoinPool pool;
    final int threads;

    public ParallelNode(ForkJoinPool pool, Function<T,R> mapper) {
        this.mapper = mapper;
        this.pool = pool;
        this.threads = 0;
    }

    public ParallelNode(int threads,Function<T,R> mapper) {
        this.mapper = mapper;
        this.pool = null;
        this.threads = threads;
    }

    @Override
    public @NotNull Class<ParallelNode> getType() {
        return ParallelNode.class;
    }

    @Override
    public @NotNull List<R> execute(StreamScope scope) {
        try {
            PartitionAction<T,R> action = new PartitionAction<>(((List<T>) scope.getItems()).spliterator(), mapper);
            if (pool == null) {
                try (ForkJoinPool otherPool = new ForkJoinPool(threads)) {
                    return otherPool.invoke(action).reversed();
                }
            }
            else {
                return pool.invoke(action).reversed();
            }
        }
        catch (RuntimeException e) {
            if (handler != null) handler.apply(e);
            return (List<R>) StreamScope.EMPTY;
        }
    }

    Function<RuntimeException,List<R>> handler;
    @Override
    public Function<RuntimeException, List<R>> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, List<R>> function) {
        handler = function;
    }
}
