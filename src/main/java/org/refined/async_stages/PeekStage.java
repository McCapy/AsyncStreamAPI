package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class PeekStage<I> extends AsyncStage<I, I> {

    final Consumer<I> consumer;

    public PeekStage(Consumer<I> consumer) {
        this.consumer = consumer;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        ((List<I>) items).forEach(consumer);
        return items;
    }
}
