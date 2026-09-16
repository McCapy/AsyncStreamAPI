# AsynchronousStream-API


>
>[!WARNING]
> If you're using code generation you **MUST** make sure to include the annotation processor. Below is how to use both for your projects. Make note you must have the dependency for the AsyncStreamAPI as well for this to be validated.
> ```xml
> <build>
>        <plugins>
>            <plugin>
>                <groupId>org.apache.maven.plugins</groupId>
>                <artifactId>maven-compiler-plugin</artifactId>
>                <version>3.13.0</version>
>                <configuration>
>                    <annotationProcessorPaths>
>                        <path>
>                            <groupId>org.refined.executable</groupId>
>                            <artifactId>AnnotationProcessor</artifactId>
>                            <version>1.0-SNAPSHOT</version>
>                        </path>
>                    </annotationProcessorPaths>
>                </configuration>
>            </plugin>
>        </plugins>
>    </build>
> ```
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
> | `AsyncStream.of(Collection<R>)`    | Returns a stream of type `R`    |
> | `AsyncStream.of(R... values)`      | Returns a stream of type `R`    | 
| | `AsyncStream.ofEmpty()`            | Returns a stream of type `Void` |  

> [!NOTE]
> An example using all of these will be supplied below.\
> If you have any questions regarding any, whether that\
> be usage or functionality, a deep explanation will be\
> supplied, as well as a deep, realistic use-case.

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
// This multiplies each item by two in a separate stream,
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
## Public API
This is probably the Hardest part of the AsynchronousStreamAPI
It allows you to generate just about anything in the AsyncStream 
class, Naturally this does come with some setbacks, Namely: we 
must use annotation processing which can be a bit hard to use at times

The simple way to use it though will be shown below.

```java
public class MethodHolder {
    @Generates("""
              @Artificial("Made via generator")
              public AsyncStream<T> example(int value) {
                  System.out.println("Received value: " + value);
                  return this;
              }
              """)
    public void doNothing() { }
}
```

You're going to see a pretty big issue here, the annotation is attached 
to a method that does... nothing?? Yes, unfortunately it must (due to how 
annotations work in general), although it can be attached to fields,
classes, and just about anything, it is not a repeatable annotation, however.
While it is a planned change to make it repeatable it isn't feasible right now.

Although instead of having a method that does nothing, you can instead attach it
to a class, so you're not making no-opp methods & fields.

## Why use it?

The reason this part of the API was made, is you get to make your own
methods without the need to extend & override or create more, this is
mainly meant to be a convenience thing, ALTHOUGH, it also allows for
your own, custom, functionality to be added to the AsyncStreamAPI.

The way we use it in this case, is simply executing the maven goal,
```bat
mvn clean install
```
this, will cause the changes you made to appear, allowing you to use
the methods you (and others) have generated.
