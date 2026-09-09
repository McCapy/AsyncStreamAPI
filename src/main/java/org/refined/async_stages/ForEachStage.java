package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class ForEachStage<I, O> extends AsyncStage<I, O> {
    final Consumer<I> consumer;

    public ForEachStage(Consumer<I> consumer) {
        this.consumer = consumer;
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        ((List<I>) items).forEach(consumer);
        return AsynchronousStream.EMPTY;
    }
}
