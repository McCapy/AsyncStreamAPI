import org.refined.AsyncStream;

void main() throws InterruptedException {
    AsyncStream<String> stream =
        new AsyncStream<>(1, 2, 3, 4, 5)
            .fork("test",items ->
                new AsyncStream<>(items)
                    .map(String::valueOf)
            )
            .submit(() -> System.out.println("Post fork tasks executing."))
            .submit(() -> System.out.println("Collecting..."))
            .collect("test",String.class)
            .submit(() -> System.out.println("Collection completed."))
            .start();
    Thread.sleep(4_000L);
    System.out.print("\nResult joined from child thread: " + Arrays.toString(stream.join()));
}