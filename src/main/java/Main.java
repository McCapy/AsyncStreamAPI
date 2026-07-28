import org.refined.AsyncStream;
import org.refined.Range;

void main() throws InterruptedException {
    AsyncStream<Integer> stream =
        new AsyncStream<>(Range.of(1,5))
            .map((item) -> item)
            .peek(System.out::println)
            .start();
    Thread.sleep(1500L);
    System.out.println("\nResult joined from child thread: " + Arrays.toString(stream.join()));

}