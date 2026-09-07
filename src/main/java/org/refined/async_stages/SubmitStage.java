package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;

public class SubmitStage<I> extends AsyncStage<I, I> {
    final Runnable runnable;

    public SubmitStage(Runnable runnable) {
        this.runnable = runnable;
    }

    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        runnable.run();
        return items;
    }
}
