package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class ReplaceStage<I> extends AsyncStage<I, I> {

    final Predicate<I> predicate;
    final Supplier<I> supplier;

    public ReplaceStage(Predicate<I> predicate, Supplier<I> supplier) {
        this.predicate = predicate;
        this.supplier = supplier;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        I holder = supplier.get();
        return ((List<I>) items).stream().map((item) -> predicate.test(item) ? holder : item).toList();
    }
}
