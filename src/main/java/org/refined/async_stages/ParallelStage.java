package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class ParallelStage<I, O> extends AsyncStage<I, O> {

    final Function<I, O> function;

    public ParallelStage(Function<I, O> mapper) {
        this.function = mapper;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return ((List<I>) items).parallelStream().map(function).toList();
    }
}
