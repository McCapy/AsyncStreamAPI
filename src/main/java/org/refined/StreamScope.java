package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "ResultOfMethodCallIgnored", "rawtypes", "CallToPrintStackTrace", "unchecked"})

public final class StreamScope {
    private static final byte UNSTARTED = 2;
    private static final byte STARTED = 1;
    private static final byte COMPLETED = 0;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ArrayList<TaskNode> tasks = new ArrayList<>(2);
    public final LinkedHashMap<String,AsyncStream<Object>> forkMap = new LinkedHashMap<>(4);

    private Thread worker;
    private List<Object> current;
    public int taskIndex = 0;
    private String name = "<none>";

    public Runnable onCancel;
    void onCancel(Runnable runnable) {
        onCancel = runnable;
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

    public List<Object> getItems() {
        return current;
    }
    public Thread getWorker() {
        return worker;
    }
    public List<TaskNode> getTasks() {
        return Collections.unmodifiableList(tasks);
    }
    public String getId() {
        return name;
    }

    // Operations
    public void start() {
        if (!isUnstarted()) return;
        if (onStart != null) onStart.run();
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
    public List<Object> collect(String... ids) {
        List<Object> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            Collections.addAll(result, forkMap.remove(id).toArray(Object[]::new)); // fix this (.toArray())
        }
        return result;
    }
    public List<Object> gather() {
        List<Object> result = new ArrayList<>(forkMap.size());
        for (AsyncStream<Object> val : forkMap.values()) {
            Collections.addAll(result, val.toArray(Object[]::new));
        }
        forkMap.clear();
        return result;
    }
    // Operations

    public void joinForks() {
        joinForks(this);
    }
    private void joinForks(StreamScope scope) {
        for (AsyncStream<Object> stream : scope.forkMap.values()) {
            try {
                Object[] result = stream.toArray(Object[]::new);
                stream.scope().joinForks(stream.scope());
            }
            catch (RuntimeException ignored) {
            }
        }
    }

    private static final ThreadFactory FACTORY = Thread.ofVirtual().factory();
    public static final List<Object> EMPTY = new ArrayList<>(1);

    public void run() {
        taskIndex = 0;
        latch.countDown();
        worker = FACTORY.newThread(() -> {
            while (taskIndex < tasks.size()) {
                if (isCancelled()) return;
                try {
                    current = tasks.get(taskIndex++).execute(this);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    this.cancel();
                }
            }
            if (onComplete != null) onComplete.accept(current);
            latch.countDown();
        });
        if (name != null) {
            worker.setName(name);
        }
        worker.start();

    }

    public void addTask(TaskNode... nodes) {
        taskIndex += nodes.length;
        tasks.addAll(List.of(nodes));
    }
    public void addTasks(Collection<TaskNode> nodes) {
        taskIndex += nodes.size();
        tasks.addAll(nodes);
    }
    public void addTask(TaskNode node, int index) {
        taskIndex++;
        tasks.add(index, node);
    }
    public StreamScope setTask(int index,TaskNode node) {
        tasks.set(index,node);
        return this;
    }

    public void setName(String id) {
        this.name = id;
    }

    public Consumer<List<Object>> onComplete;
    public Runnable onStart;

    public void onComplete(Consumer<List<Object>> onComplete) {
        this.onComplete = onComplete;
    }
    public void onComplete(Runnable runnable) {
        this.onComplete = (items) -> runnable.run();
    }
    public void onStart(Runnable onStart) {
        this.onStart = onStart;
    }

    public StreamScope reset() {
        StreamScope scope = new StreamScope();
        scope.onCancel = onCancel;
        scope.onStart = onStart;
        scope.onComplete = onComplete;
        scope.addTasks(this.getTasks());
        scope.setName(this.getId());
        return scope;
    }

    private static final RuntimeException ERROR = new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskNode.");
    public <T> void injectErrorHandling(Function<RuntimeException,List<T>> function) {
        ((TaskNode<T>) tasks.get(taskIndex - 2)).setHandler(function);
    }
    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw ERROR;
    }
}
