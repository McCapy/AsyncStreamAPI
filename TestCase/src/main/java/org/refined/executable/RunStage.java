package org.refined.executable;

import org.jetbrains.annotations.NotNull;
import org.refined.AsyncStage;
import org.refined.AsynchronousStream;
import org.refined.annotation_processor.Generates;

import java.util.List;

@Generates(
        generates = """
                   public AsyncStream<T> run(Runnable runnable) {
                      return (AsyncStream<T>) super.checkedWrap(new RunStage<T>(runnable));
                   }
                   """,
        imports = "import org.refined.executable.RunStage;"
)
public class RunStage<I> extends AsyncStage<I,I> {
    final Runnable runnable;
    public RunStage(Runnable runnable) {
        this.runnable = runnable;
    }
    @Override
    public @NotNull List<?> compute(@NotNull AsynchronousStream<?> stream, @NotNull List<?> items) {
        runnable.run();
        return items;
    }
}
