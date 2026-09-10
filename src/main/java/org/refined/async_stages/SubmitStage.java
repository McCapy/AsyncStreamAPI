package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;

public class SubmitStage<I> extends AsyncStage<I, I> {
    final Runnable runnable;

    public SubmitStage(Runnable runnable) {
        this.runnable = runnable;
    }

    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        runnable.run();
        return items;
    }
}
