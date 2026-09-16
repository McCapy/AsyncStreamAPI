
import org.refined.AsyncStream;

public class Tester {
    public static void main(String[] args) {
        AsyncStream<Integer> stream =
            new AsyncStream<>(1,2,3,4,5)
                .map(item -> item * 2)
                .run(() -> System.out.println("I worked!"))
                .start();
        stream.toList();
    }
}
