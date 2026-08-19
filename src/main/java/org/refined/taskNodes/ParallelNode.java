package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;
import org.refined.PartitionAction;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.IntFunction;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ParallelNode<T,R> implements TaskNode<R> {
    final IntFunction<R[]> array;
    final Function<T,R> mapper;
    ForkJoinPool pool;

    public ParallelNode(IntFunction<R[]> array,ForkJoinPool pool, Function<T,R> mapper) {
        this.array = array;
        this.mapper = mapper;
        this.pool = pool;
    }

    public ParallelNode(IntFunction<R[]> array,Function<T,R> mapper) {
        this.array = array;
        this.mapper = mapper;
        this.pool = null;
    }

    @Override
    public Class<ParallelNode> getType() {
        return ParallelNode.class;
    }

    @Override
    public R[] execute(StreamScope scope) {
        try {
            pool = pool == null ? new ForkJoinPool(8) : pool;
            return pool.invoke(new PartitionAction<>(Arrays.spliterator((T[]) scope.getItems()), mapper, array));
        }
        catch (RuntimeException e) {
            if (handler != null) handler.apply(e);
            return null;
        }
    }

    Function<RuntimeException,R[]> handler;
    @Override
    public Function<RuntimeException, R[]> getHandler() {
        return handler;
    }

    @Override
    public void setHandler(Function<RuntimeException, R[]> function) {
        handler = function;
    }
}
