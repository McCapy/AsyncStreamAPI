package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

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
    private Object[] current;
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

    public Object[] getItems() {
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
    public Object[] join() {
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
            e.printStackTrace();
            return null;
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
    public Object[] collect(Collection<String> ids) {
        List<AsyncStream<Object>> streams = new ArrayList<>(forkMap.values());
        String[] idArray = ids.toArray(String[]::new);
        Object[] results = new Object[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            results[i] = forkMap.get(idArray[i]).toArray(Object[]::new);
        }
        return results;
    }
    public Object[] collect(String... ids) {
        List<AsyncStream<Object>> streams = new ArrayList<>(forkMap.values());
        int len = 0;
        for (AsyncStream<Object> stream : streams) {
            len += stream.toArray(Object[]::new).length;
        }
        Object[] result = new Object[len];
        int currentPlace = 0;
        for (String id : ids) {
            for (Object o : forkMap.get(id).toArray(Object[]::new)) {
                result[currentPlace] = o;
                currentPlace++;
            }
        }
        return result;
    }
    public <T> T[] gather(IntFunction<T[]> accumulator) {
        List<AsyncStream<Object>> streams = new ArrayList<>(forkMap.values());
        int result = 0;
        for (AsyncStream<Object> objectAsyncStream : streams) {
            result += objectAsyncStream.toArray(Object[]::new).length;
        }
        T[] results = accumulator.apply(result);
        int current = 0;
        for (AsyncStream<Object> stream : streams) {
            Object[] internalResult = stream.toArray(Object[]::new);
            for (T object : (T[]) internalResult) {
                results[current] = object;
                current++;
            }
        }
        return results;
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

    public Consumer<Object[]> onComplete;
    public Runnable onStart;

    public void onComplete(Consumer<Object[]> onComplete) {
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
    public <T> void injectErrorHandling(Function<RuntimeException,T[]> function) {
        ((TaskNode<T>) tasks.get(taskIndex - 2)).setHandler(function);
    }
    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw ERROR;
    }
}
