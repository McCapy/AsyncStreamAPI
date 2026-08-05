package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "ResultOfMethodCallIgnored", "rawtypes", "MethodDoesntCallSuperMethod", "unchecked"})
public final class StreamScope {
    private static final byte UNSTARTED = 2;
    private static final byte STARTED = 1;
    private static final byte COMPLETED = 0;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ArrayList<TaskNode> tasks = new ArrayList<>(2);

    private Thread worker;
    private Object[] current;
    private int taskIndex = 0;
    private String name;

    private Consumer<RuntimeException> onCancel;
    void onCancel(Runnable runnable) {
        onCancel = _ -> runnable.run();
    }
    void onCancel(Consumer<RuntimeException> consumer) {
        onCancel = consumer;
    }

    private volatile RuntimeException error;

    public RuntimeException getError() { return error; }

    public void setError(RuntimeException exception) {
        if (error == null) {
            error = exception;
        }
    }
    public void resetError() {
        error = null;
    }

    public boolean isUnstarted() {
        return latch.getCount() == UNSTARTED;
    }

    public boolean isStarted() {
        return latch.getCount() <= STARTED;
    }

    public boolean isCompleted() {
        return latch.getCount() == COMPLETED;
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    void start() {
        if (!isUnstarted()) return;
        if (onStart != null) onStart.run();
        run();
    }

    Object[] join() {
        return join(-1L);
    }

    Object[] join(long ms) {
        if (isCancelled()) return null;
        if (!isStarted()) start();
        try {
            if (ms <= 0) {
                latch.await();
            }
            else {
                latch.await(ms, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            setError(new RuntimeException(e.getMessage()));
            return null;
        }
        return current;
    }

    public void cancel() {
        if (isCompleted()) return;
        if (isCancelled()) return;
        if (onCancel != null) onCancel.accept(error);
        taskIndex = tasks.size();
        cancelled.set(true);
        for (long i = latch.getCount(); i > 0; i--) {
            latch.countDown();
        }
    }

    public void run() {
        if (name != null) {
            worker.setName(name);
        }
        latch.countDown();
        worker = Thread.ofVirtual().start(() -> {
            while (taskIndex < tasks.size()) {
                if (isCancelled()) return;
                try {
                    current = tasks.get(taskIndex++).execute(this);
                } catch (RuntimeException e) {
                    this.setError(e);
                }
                if (taskIndex == tasks.size()) break;
            }
            latch.countDown();
            if (onComplete != null) onComplete.accept(current);
        });

    }

    public void addTask(TaskNode node) {
        tasks.add(node);
    }

    public void addTasks(TaskNode... nodes) {
        tasks.addAll(List.of(nodes));
    }

    public void addTasks(Collection<TaskNode> nodes) {
        tasks.addAll(nodes);
    }

    public void insertTask(TaskNode node, int index) {
        tasks.add(index, node);
    }

    public void named(String id) {
        this.name = id;
    }

    public Object[] getItems() {
        return current;
    }

    public Thread getWorker() {
        return worker;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public List<TaskNode> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public String getId() {
        return name;
    }

    public void modifyIndex(int edit) {
        taskIndex += edit;
    }

    public StreamScope setTask(int index,TaskNode node) {
        tasks.set(index,node);
        return this;
    }

    Consumer<Object[]> onComplete;
    Runnable onStart;

    public Consumer<?> onComplete() {
        return onComplete;
    }

    public <T> void onComplete(Consumer<T[]> onComplete) {
        this.onComplete = (Consumer<Object[]>) ((Object) onComplete);
    }

    public void onComplete(Runnable runnable) {
        this.onComplete = (items) -> runnable.run();
    }

    public Runnable onStart() {
        return onStart;
    }

    public void onStart(Runnable onStart) {
        this.onStart = onStart;
    }

    public Consumer<RuntimeException> onCancel() {
        return onCancel;
    }

    public Map<String,Map.Entry<AsyncStream<Object>,Class<Object>>> identityMap = new HashMap<>();
    public Object[] collect(String id) { // What the fuck am I doing.
        return identityMap.get(id).getKey().join();
    }

    @Override
    public Object clone() {
        StreamScope scope = new StreamScope();
        scope.addTasks(this.getTasks());
        scope.named(this.getId());
        scope.identityMap = identityMap;
        return scope;
    }
}
