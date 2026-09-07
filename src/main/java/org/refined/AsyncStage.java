package org.refined;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.async_stages.YieldStage;

import java.util.*;

@SuppressWarnings({"CallToPrintStackTrace", "unused"})
public abstract class AsyncStage<I,O> {

    public abstract @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException;

    protected boolean catching = false;
    protected boolean failed = false;
    protected AsyncStage<?,?> next;

    public final List<?> start(AsynchronousStream<?> stream, List<?> items) {
        if (failed) return find().advance(stream,items);
        try {
            return advance(stream, compute(stream, items));
        } catch (RuntimeException e) {
            if (next != null) next.failed = true;
            if (catching) return advance(stream,((YieldStage<?>)find()).fn.apply(e));
            else {
                e.printStackTrace();
                return AsynchronousStream.EMPTY;
            }
        }
    }
    private AsyncStage<?,?> find() {
        AsyncStage<?,?> current = next;
        while (!(current instanceof YieldStage<?>)) current = current.next;
        return current;
    }
    private List<?> advance(AsynchronousStream<?> scope,List<?> items) {
        if (next == null) return items;
        this.next.catching = catching;
        return next.start(scope, items);
    }
}
