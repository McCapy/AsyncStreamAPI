import org.refined.AsyncStream;
import org.refined.AsynchronousStream;

@SuppressWarnings({"unused"})
void main() {
    AsynchronousStream<Integer> stream =
            new AsyncStream<>(1,2,3);
}
