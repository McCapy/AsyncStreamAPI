package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class FlatMapStage<I, O> extends AsyncStage<I, O> {
    final Function<I, List<O>> function;

    public FlatMapStage(Function<I, List<O>> function) {
        this.function = function;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        return ((List<I>) items).stream().flatMap(val -> function.apply(val).stream()).toList();
    }
}
