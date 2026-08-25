package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "ResultOfMethodCallIgnored", "rawtypes", "CallToPrintStackTrace", "unchecked", "InstantiatingAThreadWithDefaultRunMethod"})

public final class StreamScope {

    @Override
    public String toString() {
        return
           """
           \nStreamScope information:
           Cancelled: %s
           Started: %s
           Unstarted: %s
           Completed: %s
           Forks: %s
           Tasks: %s
           ThreadWorker: %s
           TaskIndex: %s
           Name: %s
           """.formatted(cancelled.get(),isStarted(),isUnstarted(),isCompleted(),forkMap.values(),tasks,worker,taskIndex, Optional.ofNullable(worker.getName()).orElse("N/A"));
    }

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ArrayList<TaskNode> tasks = new ArrayList<>(2);
    public final LinkedHashMap<String,AsynchronousStream<Object>> forkMap = new LinkedHashMap<>(4);

    public Thread worker;
    private List<Object> current;
    public int taskIndex = 0;

    public Runnable onCancel;
    void onCancel(Runnable runnable) {
        onCancel = runnable;
    }

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

    // Operations
    public void start() {
        if (!isUnstarted()) return;
        run();
    }
    public List<Object> join() {
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
                latch.await(ms, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return EMPTY;
        }
        joinForks();
        return current;
    }
    public void cancel() {
        if (isCompleted()) return;
        if (isCancelled()) return;
        if (onCancel != null) onCancel.run();
        taskIndex = tasks.size();
        cancelled.set(true);
        for (long i = latch.getCount(); i > 0; i--) {
            latch.countDown();
        }
    }
    public List<Object> collect(List<String> ids) {
        return ids.parallelStream()
            .flatMap(id -> forkMap.remove(id).toList().stream())
            .toList();
    }
    public List<Object> gather() {
        List<Object> result =
            new ArrayList<>(forkMap.values()).parallelStream()
                .flatMap(stream -> stream.toList().stream())
                .toList();
        forkMap.clear();
        return result;
    }
    // Operations

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

    private static final ThreadFactory factory = Thread.ofVirtual().factory();
    public static final List<Object> EMPTY = new ArrayList<>(1);

    public void run() {
        taskIndex = 0;
        latch.countDown();
        String name = "N/A";
        if (worker != null) name = worker.getName();
        worker = factory.newThread(() -> {
            while (taskIndex < tasks.size()) {
                if (isCancelled()) return;
                try {
                    current = tasks.get(taskIndex++).execute(this,current);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    this.cancel();
                    break;
                }
            }
            latch.countDown();
        });
        worker.setName(name);
        worker.start();

    }

    public void addTask(TaskNode<?>... nodes) {
        taskIndex += nodes.length;
        tasks.addAll(List.of(nodes));
    }

    public void addTasks(ArrayList<TaskNode> nodes) {
        taskIndex += nodes.size();
        tasks.addAll(nodes);
    }

    public StreamScope reset() {
        StreamScope scope = new StreamScope();
        scope.onCancel = onCancel;
        scope.addTasks(tasks);
        scope.worker = new Thread(Optional.ofNullable(worker.getName()).orElse("N/A"));
        this.cancel();
        return scope;
    }

    private static final RuntimeException ERROR = new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskNode.");
    public <T> void injectErrorHandling(BiFunction<RuntimeException, StreamScope, List<?>> function) {
        ((TaskNode<T>) tasks.get(taskIndex - 1)).handler(function);
    }
    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw ERROR;
    }
}
