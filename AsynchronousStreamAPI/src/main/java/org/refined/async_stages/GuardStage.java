package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;

public class GuardStage<I> extends AsyncStage<I,I> {

    public GuardStage() { }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return items;
    }
}
