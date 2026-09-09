package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Predicate;

@SuppressWarnings("unchecked")
public class FilterStage<I> extends AsyncStage<I, I> {
    final Predicate<I> predicate;

    public FilterStage(Predicate<I> predicate) {
        this.predicate = predicate;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        return ((List<I>) items).stream().filter(predicate.negate()).toList();
    }
}
