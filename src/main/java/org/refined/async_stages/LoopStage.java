package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class LoopStage<I> extends AsyncStage<I, I> {

    final int repetitions;
    final Function<List<I>, AsynchronousStream<I>> streamFunction;

    public LoopStage(int repetitions, Function<List<I>, AsynchronousStream<I>> stream) {
        this.repetitions = repetitions;
        this.streamFunction = stream;
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        List<?> result = items;
        for (int i = 0; i < repetitions; i++) result = streamFunction.apply((List<I>) result).toList();
        return result;
    }
}
