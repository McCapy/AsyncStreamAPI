import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    AsyncStream<Integer> stream =
        new AsyncStream<>(1,2,3,4,5,6,7,8,9,10)
            .guard(self -> self
                .submit(() -> { throw new RuntimeException("Failed"); })
            )
            .yield((err) -> {
                err.printStackTrace();
                System.err.println("Failed but offering new values.");
                return Arrays.asList(5,4,3,2,1);
            })
            .start();
    System.out.println(stream.toList());
    // dun dun dun
}
