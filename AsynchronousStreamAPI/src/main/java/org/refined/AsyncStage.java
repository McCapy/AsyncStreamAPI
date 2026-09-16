package org.refined;

import org.jetbrains.annotations.NotNull;
import org.refined.async_stages.GuardStage;
import org.refined.async_stages.YieldStage;
import org.refined.exceptions.MissingSyntaxException;

import java.util.*;

@SupportsGeneration
public abstract class AsyncStage<I,O> {

    public abstract @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items);

    protected AsyncStage<?,?> next;

    public final List<?> start(AsynchronousStream<?> stream, List<?> items,boolean catching) {
        if (this instanceof GuardStage<?>) catching = true;
        try {
            try {
                return advance(stream, compute(stream, items),catching);
            } catch (RuntimeException e) {
                if (catching) {
                    try {
                        while (!(next instanceof YieldStage<?>)) next = next.next;
                    } catch (NullPointerException e1) {
                        throw new MissingSyntaxException("No accompanying Yield to the given Guard.", e1);
                    }
                    return advance(stream, ((YieldStage<?>) next).fn.apply(e),false);
                }
                new MissingSyntaxException("An error that wasn't caught was thrown.",e).printStackTrace();
                stream.cancel();
                return AsynchronousStream.EMPTY;
            }
        } catch (MissingSyntaxException e) {
            stream.cancel();
            e.printStackTrace();
            return AsynchronousStream.EMPTY;
        }
    }

    private List<?> advance(AsynchronousStream<?> scope,List<?> items,boolean catching) {
        if (next == null) return items;
        return next.start(scope, items,catching);
    }
}
