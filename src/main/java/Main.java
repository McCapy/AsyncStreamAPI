import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    AsyncStream<Integer> result =
        new AsyncStream<>(1,2,3,4,5)
            .onStart(() -> System.out.println("Started"))
            .map(item -> item * 2)
            .onComplete(() -> System.out.println("Completed"))
            .onCancel(err -> {
                System.out.println("Dun Dun Dun");
            })
            .onCancel(() -> System.out.println("Other"))
            .start();
    System.out.println(result.toList());
}
