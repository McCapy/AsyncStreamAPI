package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;
import org.refined.exceptions.MissingSyntaxException;

import java.util.List;

public class GuardStage<I> extends AsyncStage<I,I> {

    public GuardStage() { }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException {
        if (!catching) {
            catching = true;
            return items;
        }
        new MissingSyntaxException("Incorrect syntax regarding guard & yields").printStackTrace();
        scope.cancel();
        return AsynchronousStream.EMPTY;
    }
}
