package org.refined;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "ResultOfMethodCallIgnored", "rawtypes", "MethodDoesntCallSuperMethod", "CallToPrintStackTrace", "unchecked"})
public final class StreamScope implements Cloneable{
    private static final byte UNSTARTED = 2;
    private static final byte STARTED = 1;
    private static final byte COMPLETED = 0;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(2);
    private final ArrayList<TaskNode> tasks = new ArrayList<>(2);

    private Thread worker;
    private Object[] current;
    public int taskIndex = 0;
    private String name;

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

    private static final RuntimeException ERROR =
            new RuntimeException("Operations cannot be added post-start, unless enacted by a TaskNode.");
    public <T> void injectErrorHandling(Function<RuntimeException,T[]> function) {
        ((TaskNode<T>) tasks.get(taskIndex - 2)).setHandler(function);
    }

    void check() throws RuntimeException {
        if (!this.isUnstarted()) throw ERROR;
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
            e.printStackTrace();
            return null;
        }
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

    public void run() {
        taskIndex = 0;
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
                    e.printStackTrace();
                    this.cancel();
                }
                if (taskIndex == tasks.size()) break;
            }
            latch.countDown();
            if (onComplete != null) onComplete.accept(current);
        });

    }


    public void addTask(TaskNode... nodes) {
        taskIndex += nodes.length;
        tasks.addAll(List.of(nodes));
    }

    public void addTasks(Collection<TaskNode> nodes) {
        taskIndex += nodes.size();
        tasks.addAll(nodes);
    }

    public void insertTask(TaskNode node, int index) {
        taskIndex++;
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

    public List<TaskNode> getTasks() {
        return Collections.unmodifiableList(tasks);
    }

    public String getId() {
        return name;
    }

    public StreamScope setTask(int index,TaskNode node) {
        tasks.set(index,node);
        return this;
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

    public LinkedHashMap<String,AsyncStream<Object>> forkMap = new LinkedHashMap<>(4);
    public Object[] collect(Collection<String> ids) {
        String[] idArray = ids.toArray(String[]::new);
        Object[] results = new Object[ids.size()];
        for (int i = 0; i < ids.size(); i++) {
            results[i] = forkMap.get(idArray[i]).join();
        }
        return results;
    }
    public Object[] collect(String... ids) {
        Object[] results = new Object[ids.length];
        for (int i = 0; i < ids.length; i++) {
            results[i] = forkMap.get(ids[i]).join();
        }
        return results;
    }

    public Object[] gather() {
        List<AsyncStream<Object>> streams = new ArrayList<>(forkMap.values());
        int result = 0;
        for (AsyncStream<Object> objectAsyncStream : streams) {
            result += objectAsyncStream.join().length;
        }
        Object[] results = new Object[result];
        int current = 0;
        for (AsyncStream<Object> stream : streams) {
            Object[] internalResult = stream.join();
            for (Object object : internalResult) {
                results[current] = object;
                current++;
            }
        }
        return results;
    }

    @Override
    public Object clone() {
        StreamScope scope = new StreamScope();
        scope.addTasks(this.getTasks());
        scope.named(this.getId());
        scope.forkMap = forkMap;
        return scope;
    }
}
