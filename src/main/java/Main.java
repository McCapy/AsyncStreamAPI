import org.refined.AsyncStream;
@SuppressWarnings({"unused"})
void main() {
    List<Integer> holder = IntStream.rangeClosed(1,25_000_000).boxed().toList();
    long ms = System.currentTimeMillis();
    List<Integer> result =
        new AsyncStream<>(holder)
            .map(item -> item * 10)
            .toList();
    System.out.println(System.currentTimeMillis() - ms + "ms");
}