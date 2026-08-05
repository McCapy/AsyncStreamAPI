package org.refined.taskNodes;

import org.refined.StreamScope;
import org.refined.TaskNode;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MapNode<T,R> implements TaskNode<R> {
    private final Function<T,R> function;

    public MapNode(Function<T,R> function) {
        this.function = function;
    }

    @Override
    public Class<MapNode> getType() {
        return MapNode.class;
    }

    @Override
    public R[] execute(StreamScope scope) {
        Object[] items = scope.getItems();
        R[] results = (R[]) new Object[items.length];
        CountDownLatch latch = new CountDownLatch(items.length);

        for (int i = 0; i < items.length; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    T input = (T) items[idx];
                    results[idx] = function.apply(input);
                }
                catch (RuntimeException error) {
                    scope.setError(error);
                    for (long count = latch.getCount(); count > 0; count--) {
                        latch.countDown();
                    }
                }
                finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            scope.setError(new RuntimeException(e.getMessage()));
            return null;
        }
        return results;
    }
}
