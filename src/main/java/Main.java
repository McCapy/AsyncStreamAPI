import org.refined.AsyncStream;

void main() {
    AsyncStream<String> stream =
        new AsyncStream<>(items)
            .forkEach(holder ->
                new AsyncStream<>(holder)
                    .map(String::valueOf)
            )
            .gather(String.class)
            .peek(System.out::println)
            .start();
    System.out.print("\nResult joined from child thread: " + Arrays.toString(stream.join()));
}
static final Integer[] items =
        new Integer[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
