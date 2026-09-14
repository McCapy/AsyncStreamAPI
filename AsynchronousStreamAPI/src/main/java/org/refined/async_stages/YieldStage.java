package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"unchecked", "unused"})
public class YieldStage<I> extends AsyncStage<I, I> {

    public final Function<RuntimeException, List<I>> fn;

    public YieldStage(Function<RuntimeException, List<I>> fn) {
        this.fn = fn;
    }

    public YieldStage(Consumer<RuntimeException> cs) {
        this.fn = err -> {
            cs.accept(err);
            return (List<I>) AsynchronousStream.EMPTY;
        };
    }

    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        return items;
    }
}
