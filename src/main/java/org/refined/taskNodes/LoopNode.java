package org.refined.taskNodes;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStream;
import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"rawtypes",  "unchecked"})
public class LoopNode<R,T> implements TaskNode<R> {

    final Function<List<T>,AsyncStream<R>> streamFunction;
    final int repetitions;
    public LoopNode(int repetitions, Function<List<T>,AsyncStream<R>> stream) {
        this.streamFunction = stream;
        this.repetitions = repetitions;
    }

    @Override
    public @NotNull Class<LoopNode> getType() {
        return LoopNode.class;
    }

    @Override
    public @NotNull List<R> execute(StreamScope scope) {
        try {
            AsyncStream<R> stream = streamFunction.apply(((List<T>) scope.getItems()));
            List<Object>[] current = new List[1];
            current[0] = scope.getItems();
            for (int i = 0; i < repetitions; i++) {
                current[0] = stream.reset().scope().setTask(0,new OfferNode<>(_ -> current[0])).join();
            }
            return (List<R>) current[0];
        } catch (RuntimeException e) {
            if (getHandler() != null) return handler.apply(e);
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
        this.handler = function;
    }
}
