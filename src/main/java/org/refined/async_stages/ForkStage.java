package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class ForkStage<I,O> extends AsyncStage<I, O> {
    final Function<List<I>,AsynchronousStream<?>> fn;
    public ForkStage(Function<List<I>,AsynchronousStream<?>> fn) {
        this.fn = fn;
    }
    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        scope.forks.add(fn.apply((List<I>) items).start());
        return AsynchronousStream.EMPTY;
    }

}
