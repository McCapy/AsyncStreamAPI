package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class StreamScope {

    /**
     * @param nodes Adds a TaskNode to the queue
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

    int end = 0;
    public void start() {
        if (isStarted()) return;
        addTask(new TaskNode.EndNode<>());
        end = taskIndex;
        run();
    }

    List<Object> join() {
        return join(-1L);
    }
    List<Object> join(long ms) {
        if (isCancelled()) return EMPTY;
        if (!isStarted()) start();
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
        cancelled.set(true);
        for (long i = latch.getCount(); i > 0; i--) {
            latch.countDown();
        }
        current = EMPTY;
    }
    // Operations

    static final List<Object> EMPTY = new ArrayList<>(1);
    static final Comparator<TaskNode> COMPARATOR = Comparator.comparingInt(TaskNode::weight);
    public void run() throws RuntimeException {
        taskIndex = 0;
        latch.countDown();
        String name = worker == null ? "N/A" : worker.getName();
        tasks.sort(COMPARATOR);
        System.out.println(tasks);
        worker = Thread.ofVirtual().factory().newThread(() -> {
            if (current == null) current = EMPTY;
            boolean catching = false;
            RuntimeException exception = new RuntimeException(" [ROOT] ");
            while (taskIndex < tasks.size()) {
                if (isCancelled()) {
                    taskIndex = end - 1;
                    while (taskIndex < tasks.size()) {
                        TaskNode<Object> other = tasks.get(taskIndex++);
                        other.params.add(exception);
                        current = other.execute(this, current);
                    }
                    break;
                }
                switch (tasks.get(taskIndex++)) {
                    case TaskNode.GuardNode node -> {
                        catching = true;
                        current = node.execute(this,current);
                    }
                    case TaskNode.YieldNode node -> {
                        catching = false;
                        node.params.add(exception);
                        exception = new RuntimeException(" [ROOT] ");
                        current = node.execute(this,current);
                    }
                    case TaskNode.EndNode node -> {
                        current = node.execute(this,current);
                        taskIndex = tasks.size();
                    }
                    case TaskNode node -> {
                        System.out.println(node.getClass().getSimpleName());
                        this.cancel();
                        try {
                            current = node.execute(this, current);
                        } catch (RuntimeException e) {
                            if (catching) {
                                exception =
                                    new RuntimeException(
                                        e.getMessage() +
                                        "\n" +
                                        Arrays.toString(e.getStackTrace()) +
                                        "\n" +
                                        exception.getMessage() +
                                        "\n" +
                                        Arrays.toString(exception.getStackTrace())
                                    );
                            }
                            else {
                                new RuntimeException("Uncaught Exception, no guard & yield clause. See documentation.",e).printStackTrace();
                            }
                        }
                    }
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
