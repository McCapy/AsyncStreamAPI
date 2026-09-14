import org.refined.AsyncStream;

public class Main {
    public static void main(String[] args) {
        AsyncStream<Integer> stream = new AsyncStream<>(1,2,3,4,5).map(item -> item * 2).start();
        stream.toList();
    }
}