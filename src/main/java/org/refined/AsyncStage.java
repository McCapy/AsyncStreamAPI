package org.refined;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.UnknownNullability;
import org.refined.async_stages.YieldStage;
import org.refined.exceptions.MissingSyntaxException;

import java.util.*;

@SuppressWarnings({"CallToPrintStackTrace", "unused"})
public abstract class AsyncStage<I,O> {

    public abstract @NotNull List<?> compute(@NotNull AsynchronousStream<?> scope, @UnknownNullability List<?> items) throws RuntimeException;

    protected boolean catching = false;
    protected boolean failed = false;
    protected AsyncStage<?,?> next;

    public final List<?> start(AsynchronousStream<?> stream, List<?> items) {
        try {
            if (failed) return find().advance(stream, items);
            try {
                return advance(stream, compute(stream, items));
            } catch (RuntimeException e) {
                if (next != null) next.failed = true;
                if (catching) return advance(stream, ((YieldStage<?>) find()).fn.apply(e));
                new MissingSyntaxException("An error that wasn't caught was thrown.",e).printStackTrace();
                return AsynchronousStream.EMPTY;
            }
        } catch (MissingSyntaxException e) {
            stream.cancel();
            e.printStackTrace();
            return AsynchronousStream.EMPTY;
        }
    }
    private AsyncStage<?,?> find() throws MissingSyntaxException {
        try {
            AsyncStage<?, ?> current = next;
            while (!(current instanceof YieldStage<?>)) {
                System.out.println("Next");
                current = current.next;
            }
            return current;
        } catch (NullPointerException e) {
            throw new MissingSyntaxException("No accompanying Yield to the given Guard.",e);
        }
    }
    private List<?> advance(AsynchronousStream<?> scope,List<?> items) {
        if (next == null) return items;
        this.next.catching = catching;
        return next.start(scope, items);
    }
}
