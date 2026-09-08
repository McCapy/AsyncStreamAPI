import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    AsyncStream<Integer> stream =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .fork(items ->
                new AsyncStream<>(items)
                    .map(item -> item * 2)
            )
            .collect(0,Integer.class)
            .start();
    System.out.println(stream.toList());

    // dun dun dun
}
