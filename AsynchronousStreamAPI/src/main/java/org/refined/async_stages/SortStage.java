package org.refined.async_stages;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@SuppressWarnings("unchecked")
public class SortStage<I> extends AsyncStage<I, I> {

    final Comparator<I> comparator;
    final boolean parallel;

    public SortStage(Comparator<I> comparator, boolean parallel) {
        this.comparator = comparator;
        this.parallel = parallel;
    }

    @Override
    @NotNull
    public List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        I[] res = (I[]) items.toArray();
        if (parallel) Arrays.parallelSort(res, comparator);
        else Arrays.sort(res, comparator);
        return Arrays.asList(res);
    }
}
