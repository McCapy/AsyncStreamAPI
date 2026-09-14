import org.refined.AsyncStream;

void main() {
    AsyncStream<Integer> stream =
            new AsyncStream<>(1,2,3,4,5)
                .map(item -> item * 2)
                .start();
    stream.toList();
}
