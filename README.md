# AsynchronousStream-API

>
> [!NOTE]
> This is using the default implementation, AsyncStream<?>\
> all of these methods can be changed by addon creators to\
> create new methods, new TaskNodes, as well as functionality.
>

## Constructors and Factory-Constructors
> | Constructor                        | Definition                      |
> |------------------------------------|---------------------------------|
> | `new AsyncStream<>()`              | Returns a stream of type `Void` |
> | `new AsyncStream<>(Collection<T>)` | Returns a stream of type `T`    |
> | `new AsyncStream<>(T...)`          | Returns a stream of type `T`    |

>
> [!NOTE]
> An example using all of these will be supplied below.\
> If you have any questions regarding any, whether that\
> be usage or functionality, a deep explanation will be\
> supplied, as well as a deep, realistic use-case.
>

> [!NOTE]
> You *MUST* remember to catch errors on potentially\
> volatile execution paths. Whenever an error is thrown\
> it will implicitly return null, not only this, but:\
> the error *should* also be caught, an error catch\
> can return values for when an error is encountered\
> which will be passed upstream whenever one is caught\
> but, it can also be used for printing error logs.\
> You can see below for documentation on error catching.

## Methods And Operations
> | Method                                                     | Type                   | Description                                                                                                                              |
> |------------------------------------------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
> | `.start()`                                                 | Status Operation       | Starts the Stream, non-stopping.                                                                                                         |
> | `.toArray()`                                               | Status Operation       | Joins the Stream, returns an array.                                                                                                      |
> | `.toArray(long ms)`                                        | Status Operation       | Joins the Stream, returns an array after `ms` regardless of completion.                                                                  |
> | `.toCollection()`                                          | Status Operation       | Joins the stream, returns a collection.                                                                                                  |
> | `.toCollection(long ms)`                                   | Status Operation       | Joins the stream, returns a collection after `ms` regardless of completion.                                                              |
> | `.toAbstract(Function<List<T>,R)`                          | Status Operation       | Joins the stream, returns `R`; which can be anything.                                                                                    |
> | `.toAbstract(long ms, Function<List<T>,R>`                 | Status Operation       | Joins the stream, returns `R` after `ms` which can be anything.                                                                          |
> | `.named(String name)`                                      | Status Operation       | Sets the name of the thread that runs the stream, useful for debugging.                                                                  |
> | `.forEach(Consumer<T>)`                                    | Intermediate Operation | Iterates over all items, applies the consumer, returns Void.                                                                             |
> | `.map(Function<T,R>)`                                      | Intermediate Operation | Iterates over the items, applies the function, and returns the result.                                                                   |
> | `.flatMap(Function<T,List<R>>)`                            | Intermediate Operation | Applies the flatmap operation over each item in the list, then returns.                                                                  |
> | `.submit(Runnable)`                                        | Intermediate Operation | Executes the runnable. Returns the previous set of items.                                                                                |
> | `.peek(Consumer<T>)`                                       | Intermediate Operation | Each item in the stream is accepted into the consumer. Returns previous items.                                                           |
> | `.filter(Predicate<T>)`                                    | Intermediate Operation | If the given predicate returns true, removes the item from the stream.                                                                   |
> | `.replace(Predicate<T>,Supplier<T>`                        | Intermediate Operation | If the given predicate returns true, replaces the item with the supplied item.                                                           |
> | `.replace(Predicate<T>,T)`                                 | Intermediate Operation | If the given predicate returns true, replaces the item with the supplied item.                                                           |
> | `.empty()`                                                 | Intermediate Operation | Removes all existing items from the Stream                                                                                               |
> | `.empty(Runnable)`                                         | Intermediate Operation | Removes all existing items from the Stream, and executes the `Runnable`.                                                                 |
> | `.empty(Consumer<List<T>>)`                                | Intermediate Operation | Steals all existing items from the stream, and applies them to the `Consumer`.                                                           |
> | `.sort(Comparator<T>)`                                     | Intermediate Operation | Sorts the existing items with respect to the `Comparator`.                                                                               |
> | `.parallelSort(Comparator<T>)`                             | Intermediate Operation | Sorts the existing items with respect to the `Comparator` parallely.                                                                     |
> | `.reversed()`                                              | Intermediate Operation | Reverses the items inside of the stream.                                                                                                 |
> | `.loop(int loops,Function<List<T>,AsynchronousStream<R>>)` | Intermediate Operation | Applies the existing items to the Function, and loops `iterations` times, passing the previous result back into the following execution. |
> | `.onComplete(Consumer<List<integer>>)`                     | Event Operation        | Executes the given consumer, accepting the result into the given consumer.                                                               |
> | `.onComplete(Runnable)`                                    | Event Operation        | Executes the given runnable after completion.                                                                                            |
> | `.onStart(Runnable)`                                       | Event Operation        | Executes the given runnable at start.                                                                                                    |
> | `.onCancel(Runnable)`                                      | Event Operation        | Executes the given runnable on cancel.                                                                                                   |
> | `.fork(String id, Function<List<T>,AsynchronousStream<?>>` | Forking Operation      | Starts a parallel fork task.                                                                                                             |
> | `.forkEach(Function<T,AsynchronousStream<?>>)`             | Forking Operation      | Starts a parallel fork task for each item inside the Stream.                                                                             |
> | `.gather(Class<T>)`                                        | Forking Operation      | Gathers ALL forked tasks. and adds the results (in the order of adding)                                                                  |           
> | `.collect(Class<T>,String... ids)`                         | Forking Operation      | Gathers the results of the supplied forked tasks in the order as supplied in the `ids` array.                                            |

## Operation Usages
#### Forking Operations
```java
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .forkEach(item -> {
                return
                    new AsyncStream<>(item)
                        .map(other -> other * 10);
            })
            .gather(Integer.class)
            .toList();
}
        // This creates 10 forks, and gathers all of them together,
        // with a result of: List<Integer>{10,20,30,40,50,60,70,80,90,100}
```
```java
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .fork("example-id",items ->
                new AsyncStream<>(items)
                    .map(item -> item * 10)
            )
            // Here is where you would do other work, that is independent of that calculation
            // Although after this work is done you can gather the
            // result of the given fork and merge it with the result of the collection.
            .collect(Integer.class,"example-id")
            .toList();
}
// This creates 1 fork, which handles the work of mapping all 5 items to <item> * 10
```
#### Intermediate Operations
```java
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .map(item -> new AsyncStream<>(item).map(other -> other * 2))
            .flatMap(stream -> stream.toList())
            .reversed()
            .toList();
}
// This multiples each item by two in a separate stream,
// and then flattens it back into AsyncStream<Integer> and then
// reverses it for the result of: List<Integer>{10,8,6,4,2}
```
```java
void main() {
    Optional<List<Integer>> result =
        new AsyncStream<>(1,2,3,4,5)
            .map(item -> item * 100)
            .toAbstract(Optional::of);
}
// Wraps the list of results in an optional. You can also do other things
// Like convert it into an Optional<Integer[]>, etc.
```
```java
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .loop(10,(items) -> new AsyncStream<>(items).map(item -> item + 1))
            .toList();
}
    // Adds 10 to every item in the stream (via a loop) and then converts it into a list.
```
```java
void main() {
    List<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .map(item -> {
                if (item == 3) throw new RuntimeException("EXAMPLE!");
                return item * 2;
            })
            .intercept((error,scope) -> {
                error.printStackTrace();
                scope.cancel();
            })
            .toList();
    // This is essentially the same as the latter, although it cancels the stream instead of returning
    // a default value, which can be useful in some cases.
}
```
```java
void main() {
    List<integer> result =
        new AsyncStream<>(1, 2, 3, 4, 5)
            .guard(self -> self
                .map(item -> {
                    if (item == 3) throw new RuntimeException("EXAMPLE!");
                    return item * 2;
                })
                .submit(() -> System.out.println("Completed Error Catch"))
            )
            .yield(err -> {
                error.printStackTrace();
                return Arrays.asList(5, 4, 3, 2, 1);
            })
            .toList();
    // In this example all this does is it guards the two methods, 
    // .map & .submit and if any of them throws errors it merges
    // all exceptions thrown, and at the yield if there is in fact
    // an error present, it will present the user with the error.
    // Although if there is no error, the code block will never
    // be executed which ensures null-safety.
    
}
```