package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class EmptyStage<I, O> extends AsyncStage<I, O> {
    final Consumer<List<I>> consumer;

    public EmptyStage(Runnable runnable) {
        this.consumer = (_ -> runnable.run());
    }

    public EmptyStage(Consumer<List<I>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        consumer.accept((List<I>) items);
        return AsynchronousStream.EMPTY;
    }
}
