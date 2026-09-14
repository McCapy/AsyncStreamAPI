package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class MapStage<I, O> extends AsyncStage<I, O> {

    final Function<I, O> function;

    public MapStage(Function<I, O> function) {
        this.function = function;
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return ((List<I>) items).stream().map(function).toList();
    }
}
