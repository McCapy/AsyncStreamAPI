package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"unchecked"})
public final class StreamScope {

    public AsyncStage<?,?> head;
    public AsyncStage<?,?> tail;

    public void wrap(AsyncStage<?,?> stage) {
        taskIndex++;
        if (head == null) {
            head = stage;
        }
        else {
            tail.next = stage;
        }
        tail = stage;
    }

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);

    public Thread worker;
    public List<Object> current;

    public int taskIndex = 0;

    public boolean isUnstarted() {
        return latch.getCount() == 2;
    }
    public boolean isStarted() {
        return latch.getCount() <= 1;
    }
    public boolean isCompleted() {
        return latch.getCount() == 0;
    }
    public boolean isCancelled() {
        return cancelled.get();
    }

    public void start() {
        if (isStarted()) return;
        taskIndex = 0;
        latch.countDown();
        worker = Thread.ofVirtual().factory().newThread(() -> {
            head.start(this,EMPTY);
            latch.countDown();
        });
        worker.start();
    }

    List<Object> join(long ms) {
        if (!isStarted()) start();
        try {
            if (ms <= 0) {
                latch.await();
            }
            else {
                if (!latch.await(ms, TimeUnit.MILLISECONDS)) return (List<Object>) EMPTY;
            }
        } catch (InterruptedException e) {
            return (List<Object>) EMPTY;
        }
        if (isCancelled()) return (List<Object>) EMPTY;
        return current;
    }
    public void cancel() {
        if (isCompleted()) return;
        if (isCancelled()) return;
        cancelled.set(true);
    }
    // Operations

    static final List<?> EMPTY = new ArrayList<>(1);
    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskStage.");
    }

}
