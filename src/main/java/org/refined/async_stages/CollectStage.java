package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;

public class CollectStage<I,O> extends AsyncStage<I,O> {
    final int index;
    public CollectStage(int index) {
        this.index = index;
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        return scope.forks.remove(index).toList();
    }
}
