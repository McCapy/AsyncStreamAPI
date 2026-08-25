import org.refined.AsyncStream;

@SuppressWarnings({"unused"})
void main() {
    List<Integer> stream =
        new AsyncStream<>(1,2,3)
            .named("example thread name")
            .map(item -> item * 10)
            .onComplete(() -> System.out.println("Completed"))
            .onStart(() -> System.out.println("Started"))
            .toList();
    System.out.println(stream);
}
