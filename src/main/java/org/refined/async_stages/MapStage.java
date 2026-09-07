package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
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
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        return ((List<I>) items).stream().map(function).toList();
    }
}
