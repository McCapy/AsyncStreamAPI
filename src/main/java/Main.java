import org.refined.AsyncStream;
import org.refined.AsynchronousStream;

@SuppressWarnings({"unused"})
void main() {
    AsyncStream<Integer> stream =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .map(item -> new AsyncStream<>(item))
            .flatMap(AsynchronousStream::toList)
            .start();
    System.out.println(stream.toList());

    // dun dun dun
}
