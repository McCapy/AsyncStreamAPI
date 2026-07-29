import org.refined.AsyncStream;
import org.refined.Range;

void main() throws InterruptedException {
    AsyncStream<Integer> stream =
        new AsyncStream<>(Range.of(1,5))
            .loop(5,
                new AsyncStream<Integer>()
                    .peek(item -> System.out.print(item+" "))
                    .map((item) -> item + 1)
                    .submit(() -> System.out.print("\nCompleted\n"))
            )
            .start();
    Thread.sleep(1500L);
    System.out.print("Result joined from child thread: " + Arrays.toString(stream.join()));

}