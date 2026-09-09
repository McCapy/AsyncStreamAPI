package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.time.Duration;
import java.util.List;

public class DelayStage<I> extends AsyncStage<I, I> {
    final Duration duration;

    public DelayStage(Duration duration) {
        this.duration = duration;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> scope, @NotNull List<?> items) throws RuntimeException {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
        return items;
    }
}
