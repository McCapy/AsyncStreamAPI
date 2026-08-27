package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"rawtypes", "unchecked", "CallToPrintStackTrace"})
public final class StreamScope {

    /**
     * @param nodes Adds a TaskNode to the stack
     */
    public void addTask(TaskNode<?>... nodes) {
        taskIndex += nodes.length;
        tasks.addAll(List.of(nodes));
    }

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);
    public final ArrayList<TaskNode> tasks = new ArrayList<>(2);
    public final LinkedHashMap<String,AsynchronousStream<Object>> forkMap = new LinkedHashMap<>(4);

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

    /**
     * Starts the AsyncStream
     */
    public void start() {
        if (isStarted()) return;
        try {
            run();
        } catch (RuntimeException e) {
            System.out.println(".start threw (which means .join did too)");
        }
    }

    List<Object> join() {
        return join(-1L);
    }
    List<Object> join(long ms) {
        if (isCancelled()) return EMPTY;
        try {
            if (!isStarted()) start();
        } catch (RuntimeException e) {
            System.out.println("Join threw!");
        }
        try {
            if (ms <= 0) {
                latch.await();
            }
            else {
                if (!latch.await(ms, TimeUnit.MILLISECONDS)) return EMPTY;
            }
        } catch (InterruptedException e) {
            return EMPTY;
        }
        joinForks();
        return current;
    }
    public void cancel() {
        if (isCompleted()) return;
        if (isCancelled()) return;
        taskIndex = tasks.size();
        cancelled.set(true);
        for (long i = latch.getCount(); i > 0; i--) {
            latch.countDown();
        }
        current = EMPTY;
    }
    // Operations

    static final List<Object> EMPTY = new ArrayList<>(1);
    public void run() throws RuntimeException {
        taskIndex = 0;
        latch.countDown();
        String name = worker == null ? "N/A" : worker.getName();
        worker = Thread.ofVirtual().factory().newThread(() -> {
            if (current == null) current = EMPTY;
            while (taskIndex < tasks.size()) {
                if (isCancelled()) break;
                System.out.println(taskIndex);
                try {
                    current = ((TaskNode<Object>) tasks.get(taskIndex++)).execute(this, current);
                } catch (TaskNode.StreamInterruption e) {
                    System.out.println("StreamException");
                    throw new RuntimeException(e);
                } catch (RuntimeException e) {
                    System.out.println("RuntimeException");
                    this.cancel();
                    e.printStackTrace();
                    break;
                }
            }
            latch.countDown();
        });
        worker.setName(name);
        worker.start();
    }

    private static final RuntimeException ERROR = new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskNode.");

    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw ERROR;
    }

    public void joinForks() {
        joinForks(this);
    }
    private void joinForks(StreamScope scope) {
        for (AsynchronousStream<Object> stream : scope.forkMap.values()) {
            try {
                StreamScope sScope = stream.scope;
                sScope.join();
                sScope.joinForks(sScope);
            }
            catch (RuntimeException _) {}
        }
    }

}
