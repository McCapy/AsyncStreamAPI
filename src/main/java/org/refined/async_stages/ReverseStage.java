package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;

public class ReverseStage<I> extends AsyncStage<I, I> {

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return items.reversed();
    }
}
