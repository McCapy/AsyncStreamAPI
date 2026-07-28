# ExecutorAPI
> The ExecutorAPI is paired with another API called the TaskNodeAPI, both of these API's respectively enable the creation of unique api.TaskNode's to fit any situation where you might need them, in the scenario where an operation with no accompanying method or api.TaskNode does not exist this will help greatly.

## Understanding
To begin we must understand what the language inside the API means, we will go over EVERY method in the ExecutorAPI and the TaskNodeAPI respectively. Some methods will be considered `volatile` or `valuetasks` this mainly applies to TaskNodes which is where this language is most common, although to users that are NOT using the TaskNodeAPI this will just be simplified to "does this return something new?" and "does this throw an error that MUST be caught?"

Although there are some exceptions to this, some methods can be marked volatile (ie their use is unstable and **CAN** throw errors, if you happen to do something in a api.TaskNode that has the potential to throw an error, that automatically makes it a `volatile` task, meaning you should implicitly either, retry the task
(explained later) **OR** do an errorCatch() and add a default value or just outright cancel the task IF there is an error (explained later as well)

Now for the actual documentation. We will start with the ExecutorAPI and move onto the TaskNodeAPI and its internals afterwards as it's quite hard to understand without the existence of the other TaskNodes and what they do to avoid pointless creation of a tasknode.

### ExecutorAPI
We will start with the creation of an Executor object. This is how you pass in `TaskNodes` among other operations.

- `new Executor()` **NON-VOLATILE, VALUETASK**
  > This is a blank executor which has an initial value of `null` meaning if you try to pass this value into an operation that is not a supplying task, then it        will likely throw.
- `new Executor(T initial)` & `new Executor(Supplier<T> initial)` **NON-VOLATILE, VALUETASK**
  > Both of these respectively initialize the Executor object with a given value at the start, so if you try to do an operation on this executor, then it will take
  > in the result of either the `supplier`, or the value `initial`

And that is all for initializations. Now we will move on to the status of an Executor.
*or if you're using the TaskNodeAPI the status of the `TaskQueue` ;)*

- `Executor#isStarted()` Checks if the executor has started.
- `Executor#isCompleted()` Checks if the executor has completed all tasks. This does not include forked tasks (unless gathered).
- `Executor#isCancelled()` Checks if the executor has been cancelled.

- `Executor#onCancel(Runnable runnable)`
  > Executes the given runnable if the Executor cancels.

We will also cover how to transform these status's.

- `Executor#cancel()` **VOLATILE**
  > Cancels the executor. this halts the worker, as well as stops execution entirely. Any results which attempt to take from the cancelled executor will return       `null`
- `Executor#start()`
  > This starts executing the Executor task, it will start at the first node and work its way to the end. It returns the instance of the Executor
- `Executor#join()`
  > This joins the result of the Executor into the parent thread, gathering the result. Keep in mind this is stopping, so only call this whenever you're certain     you NEED the result at that exact moment.
- `Executor#join(long ms)`
  > Returns null if the task was cancelled, same for the last one, and after `ms` milliseconds have elapsed, it will return null and the task will automatically     cancel itself. Although if the task is completed in the given timespan, it will return the result of the task.

And that is all for status's. Now we will move on to operations. This is where we get the ability to change data, and execute tasks.

- `Executor#loop(Function<T,R> function,int iterations)` **VALUETASK**
  > This is a very basic operation. a simple loop, it loops a specific amount of times and executes the supplied `function` taking in the previous value it          returns a new value which is passed into the next function, if there is no next function (ie all iterations are complete) it will simply return the value to be    used in the next api.TaskNode
- `Executor#loop(int loops, int tasksAhead)` **VALUETASK**
  > This is very different from the previous loop. You will notice it does not take in a function, rather it takes in a `tasksAhead` integer, this loops from the    start which is defined as the .loop()'s location in the queue, and goes ahead and repeats all tasknodes in that bound `loops` times. Or until it reaches an        error.
- `Executor#doWhile(Predicate<T> predicate, int tasksAhead)` **VALUETASK**
  > This is quite a deal different, a `do while` is a type of loop that executes first, and then checks if the condition is true. So if the condition is false       during execution, it will still execute only once. It functions very similarly to the previous terminal operation due to the fact it operates based on the        `tasksAhead` integer.

Now before we discuss future operations we MUST discuss how to handle errors properly, and safely. You are given a set of methods ranging from catching errors and supplying values to replace the nulled value, as well as things to outright retry tasks which have failed.

**NOTE** I wont cover every method from this, just because there are like 8 of them. If you want look at the src code! They all behave self explanatorily.
The ones that i explain should be enough for your needs alone though. Just keep that in mind.

- `Executor#catchError()`
  > This just checks if an error is present, if so? Cancel the task. It should be appended directly after a *'risky'* task is executed. Such as one that throws an   error.
- `Executor#catchError(Runnable runnable)`
  > Whenever an error is caught, this runnable will run. Note this does have a variant with either a Supplier or an Object which can be passed in for the value      returned whenever an error is caught to replace the null value.
- `Executor#catchError(Consumer<RuntimeException> consumer)`
  > Whenever an error is caught, the consumer will consume the error. Note this does have a variant with either a Supplier or an Object which can be passed in for   the value returned whenever an error is caught to replace the null value.
- `Executor#catchError(Supplier<T> def)`
  > Replaces the null value from the error with the result of this supplier, can also be replaced with a `T def` instead.

Now, we're doing task retrying. This set of methods does not have variants with Supplier<R>, nor Runnable's, although they will be added eventually.

- `Executor#catchRetry(int max)`
  > This defines how many times a task can be retried before it fails, and returns null.
- `Executor#catchRetry(int max, R def)`
  > This defines how many times a task can be retried, when the task fails it will return `def` instead of null.
- `Executor#catchRetry(Consumer<RuntimeException> consumer, int max)`
  > This consumes the error (if any) and retries it `max` times.
- `Executor#catchRetry(Consumer<RuntimeException> consumer, int max, R def)`
  > This consumes the error, runs `max` times, after exceding `max` it will return `def` instead of null.

Now we've finally completed error handling, we can now move on to the actual operations of an Executor.
These are the things that directly morph results and act as debugging or general purpose operations.

- `Executor#map(Function<T,R> function)`
  > This takes in the previous result from the previous task, and returns a new value from the function.
- `Executor#offer(Supplier<R> supplier)` & `Executor#offer(R item)`
  > This simply returns the result of the supplier or it returns `item` which is passed into the next tasknode.
- `Executor#peek(Consumer<T> consumer)`
  > This peeks at the current value from the previous VALUETASK and consumes it, returning the current value back, which just means it executes the operation and     returns the object passed into the operation, so it preserves the returned value perfectly.
- `Executor#filter(Predicate<T> predicate, R def)` & `Executor#filter(Predicate<T> predicate, Supplier<R> def)`
  > If the condition, `predicate`, is true; replaces it with the result of `def` or, with `def`.
- `Executor#empty(Consumer<T> consumer)`
  > Empties the result from the previous `VALUETASK` consuming it, and returning null.
- `Executor#empty(Runnable runnable)`
  > Empties the result from the previos `VALUETASK` and executing the `runnable`, returning null.
- `Executor#empty()`
  > Short form way to write,
  > ```java
  > void main() {
  >   Executor<Void> executor =
  >     new Executor(5)
  >       .map(item -> null);
  > }
  > ```
- `Executor#execute(Runnable runnable)`
  > Executes the given runnable, returns the previous `VALUETASK`'s result.
- `Executor#throwIf(Predicate<T> predicate, RuntimeException exception)`
  > Throws an exception if the `predicate` is true, the predicate accepts the result from the previous `VALUETASK`
- `Executor#cancelIf(Predicate<T> predicate)`
  > Cancels the executor if the given `predicate` is true. The predicate reads from the result of the previous `VALUETASK`
- `Executor#mapIf(Predicate<T> predicate,Function<T,R> function)`
  > Executes the mapping operation if the given `predicate` is true, it takes in the result of the previous `VALUETASK`
- `Executor#peekIf(Predicate<T> predicate,Consumer<T> consumer)`
  > Executes the given `consumer` if the given `predicate` returns true, it takes the result from the previous `VALUETASK`
- `Executor#executeIf(Predicate<T> predicate,Runnable runnable)`
  > Executes the given `runnable` if the given `predicate` returns true, it takes the reuslt from the previous `VALUETASK`
- `Executor#emptyIf(Preidcate<T> predicate,Consumer<T> consumer)`
  > Executes the given `consumer` which consumes the result of the previous `VALUETASK` the `predicate` takes in the result of the previous `VALUETASK` it also        empties the result, returning null.
- `Executor#emptyIf(Predicate<T> predicate, Runnable runnable)`
  > Executes the given `runnable` if the `predicate` is true, if the predicate happens to be true it will also empty the result.
- `Executor#emptyIf(Predicate<T> predicate)`
  > if the given `predicate` is true, it empties, returning null.
- `Executor#delay(long ms)`
  > Delays the given executor for `ms` milliseconds
- `Executor#delayIf(Predicate<T> predicate, long ms)`
  > Delays the given executor for `ms` milliseconds if the `predicate` returns true, it takes in the result of the previous `VALUETASK`
- `Executor#skipIf(Predicate<T> predicate, int skipCount)`
  > Skips `skipCount` tasks, if the given `predicate` is true, it takes in the result of the previous `VALUETASK`
- `Executor#fork(Executor<R> executor)`
  > This starts the supplied `executor` whenever it reaches the tasknode for it, you can optionally `gather` non-independent forked tasks using                        `Executor#gather()` or its sister method.
- `Executor#fork(Function<T,Executor<R>> function)`
  > This runs the function whenever the given node is executed. The value from the previous `VALUETASK` is passed in, it returns an Executor from there, which is    then started whenever the function finishes execution.
- `Executor#gather()`
  > Gathers are sidetasks and waits for all of them to complete. Returns the result of the previous `VALUETASK`
- `Executor#gather(Consumer<Object[]> consumer)`
  > Gathers all sidetasks & their results into an Object[] array, which is passed into the consumer and is consumed. It returns the result of the previous `VALUETASK`
- `Executor#gather(Function<Object[],R> function)`
  > Takes in the result of all joined sidetasks, passing them into the function and returning a value from that function.
# TaskNodeAPI
> The TaskNodeAPI is entirely arround using the TaskQueue and Executor respectively to create your own methods that can be used with the ExecutorAPI, while it       isn't very flexible, you're confined to a specific method to add both task forks (explained later) and core-tasks.

### TaskNodeAPI
- `Executor#addTask(TaskNode node)`
- `Executor#addSideTask(ForkNode node)`
  > Each of these methods respectively define how you add tasks to a taskqueue, although TaskNode... what is that? the TaskNode is a public interface which you        can implement and change its functionality. An example TaskNode will be shown below for the .map() operation, keep in mind it does not have ANY error              handling, if you make an actual TaskNode for your personal use i **HEAVILY** reccomend adding your own error handling referencing other tasknodes to infer how     to do it.
  ```java
  public class MapNodeExample implements TaskNode {
    final Function<Object,Object> function;
    public MapNodeExample(Function<Object,Object> function) {
        this.function = function;
    }
    @Override
    public Object execute(Object current, TaskQueue queue) {
        return function.apply(current);
    }

    @Override
    public Class<MapNodeExample> identity() {
        return MapNodeExample.class;
    }
  }
```java 
void main() {
  Executor<Integer> executor =
    new Executor(1)
      .addTask(new MapNodeExample((item) -> item += 2));
}
```
> While this isn't amazingly elegant, it is the only feasible way to add your own operations easily. `item` in this case automatically is defined as the previous `VALUETASK`'s result, returning something means that this is also a value task.
> I **HEAVILY** encourage you to look through other tasknodes as you will not understand how to implement your own solutions without doing so. They explain things like error handling, among other things that you MUST know, like how to correctly skip forward tasks, how to skip backwards tasks, etc.
