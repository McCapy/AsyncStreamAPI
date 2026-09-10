package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class OfferStage<I> extends AsyncStage<I, I> {

    final Function<List<I>, List<I>> function;

    public OfferStage(Function<List<I>, List<I>> function) {
        this.function = function;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return function.apply((List<I>) items);
    }
}
