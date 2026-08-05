import org.refined.AsyncStream;

void main() throws InterruptedException {
    AsyncStream<Integer> stream =
        new AsyncStream<>(1,2,3,4,5)
            .fork("test",
                new AsyncStream<Integer>()
                    .map(item -> item * 2)
                    .delay(Duration.ofMillis(5_000))
                    .submit(() -> System.out.println("Completed Fork"))
            )
            .submit(() -> System.out.println("Next operation, post fork."))
            .offer(5,4,3,2,1)
            .map(item -> item * 3)
            .forEach(System.out::println)
            .submit(() -> System.out.println("Completed ForEach, collecting."))
            .collect("test",Integer.class)
            .submit(() -> System.out.println("Collected task successfully."))
            .start();
    Thread.sleep(6_000L);
    System.out.print("Result joined from child thread: " + Arrays.toString(stream.join()));
}

/*
AsyncStream.empty()
    .<Void>forkEach(AsyncStream) numbered id? 1->n length, bad design though
    .<Void>fork(ID,AsyncStream)
    for the stream I think we just new AsyncStream<>(AsyncStream.scope().clone())
    and wrap the logic, passing in the starter value.

    .<T>collect(ID) where T Map<ID,Class<?>> which we cast whenever ID is passed to offer a return value.
    .<Object[]>gather(Optional<Class<?>>) where class is to cast from, ie Object -> ?[]

 */