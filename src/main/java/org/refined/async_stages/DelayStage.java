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
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
        return items;
    }
}
